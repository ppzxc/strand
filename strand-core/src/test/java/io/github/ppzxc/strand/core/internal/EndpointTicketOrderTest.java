package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * 번호표 취소와 기한 만료의 관측 가능한 순서와 결과를 골든 마스터로 고정한다(#202). 클라이언트가 AUTH 관문 전(번호 없는 대기열 요청)과
 * 후(번호를 받은 요청)에 걸쳐 요청(기한 유무 섞음), 번호표 취소, 번호로 취소, 응답, RESET, 시계 전진(여러 요청이 한 번에 만료되는 큰 걸음 포함),
 * 닫기, drain을 고정 시드로 무작위로 돌리고, 이벤트와 꺼낸 메시지의 SHA-256만 비교한다. 골든은 자료구조를 바꾸기 전 구현이 낸 값이다.
 *
 * <p>골든을 다시 만드는 방법은 {@link Golden}을 본다.
 */
class EndpointTicketOrderTest {

    private static final String GOLDEN_RESOURCE = "ticket-order-golden.txt";
    private static final int SCENARIOS = 400;
    private static final int OPERATIONS = 50;
    private static final int REQUEST_TYPE = 0x0100;

    private static void states(StringBuilder trace, Endpoint endpoint, List<Long> tickets) {
        for (long ticket : tickets) {
            trace.append("state ")
                    .append(ticket)
                    .append(' ')
                    .append(endpoint.requestState(ticket))
                    .append('\n');
        }
        trace.append("open ").append(endpoint.openStreamCount()).append('\n');
    }

    /** 꺼낸 요청 프레임의 스트림 번호를 {@code sentStreams}에 모은다. 상대가 응답하거나 RESET할 수 있는 번호다. */
    private static void append(StringBuilder trace, Endpoint endpoint, List<Long> tickets, List<Long> sentStreams) {
        states(trace, endpoint, tickets);
        for (Event event : endpoint.drainEvents()) {
            trace.append("event ")
                    .append(event.kind())
                    .append(' ')
                    .append(event.streamId())
                    .append(' ')
                    .append(event.status())
                    .append(' ')
                    .append(event.certainty())
                    .append('\n');
        }
        for (OutboundMessage message : endpoint.drainMessages()) {
            trace.append("message ")
                    .append(message.kind())
                    .append(' ')
                    .append(Arrays.toString(message.bytes()))
                    .append('\n');
            byte[] bytes = message.bytes();
            if (bytes.length >= 16 && Frames.readUnsignedShort(bytes, 4) == REQUEST_TYPE) {
                sentStreams.add(java.nio.ByteBuffer.wrap(bytes, 8, 8).getLong());
            }
        }
        states(trace, endpoint, tickets);
    }

    private static long streamOf(Random random, List<Long> sentStreams) {
        return sentStreams.isEmpty() ? 0 : sentStreams.get(random.nextInt(sentStreams.size()));
    }

    private static String scenario(long seed) {
        Random random = new Random(seed);
        Endpoint endpoint = Endpoint.client(EndpointConfig.defaults());
        endpoint.receive(Frames.hello(EndpointConfig.defaults()));
        endpoint.receive(Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        endpoint.drainMessages();
        // 이 걸음에서 AUTH 관문을 통과한다. 그 전의 요청은 번호 없는 대기열에 있다.
        int authAtStep = random.nextInt(OPERATIONS);
        // 이 걸음에서 시계를 크게 밀어 여럿을 만료시키고 곧바로 닫는다(만료 중 닫힘 경로).
        int expireAndCloseAtStep = random.nextInt(OPERATIONS * 3);
        StringBuilder trace = new StringBuilder();
        List<Long> tickets = new ArrayList<>();
        List<Long> sentStreams = new ArrayList<>();
        for (int step = 0; step < OPERATIONS; step++) {
            if (step == authAtStep) {
                endpoint.receive(Frames.control(FrameType.AUTH, Flags.ACK, new byte[0]));
            }
            if (step == expireAndCloseAtStep) {
                endpoint.advance(70);
                endpoint.close(random.nextBoolean() ? ErrorCode.NO_ERROR : ErrorCode.PROTOCOL_ERROR);
            }
            switch (random.nextInt(13)) {
                case 0:
                case 1:
                    tickets.add(endpoint.request(REQUEST_TYPE, new byte[random.nextInt(8)]));
                    break;
                case 2:
                case 3:
                    tickets.add(endpoint.request(REQUEST_TYPE, new byte[random.nextInt(8)], 1 + random.nextInt(60)));
                    break;
                case 4:
                    if (!tickets.isEmpty()) {
                        endpoint.cancelRequest(tickets.get(random.nextInt(tickets.size())));
                    }
                    break;
                case 5:
                    endpoint.cancel(streamOf(random, sentStreams));
                    break;
                case 6:
                    if (!sentStreams.isEmpty()) {
                        endpoint.receive(Frames.response(REQUEST_TYPE, streamOf(random, sentStreams), 0, new byte[0]));
                    }
                    break;
                case 7:
                    endpoint.advance(random.nextInt(random.nextBoolean() ? 20 : 150));
                    break;
                case 8:
                case 11:
                    append(trace, endpoint, tickets, sentStreams);
                    break;
                case 9:
                    // 상대 CLOSE: last_stream_id보다 큰 번호의 내 요청이 로컬 UNAVAILABLE로 끝난다.
                    endpoint.receive(Frames.close(ErrorCode.NO_ERROR, streamOf(random, sentStreams)));
                    break;
                case 10:
                    // 상대의 max_frame_size를 넘는 요청. 관문 뒤면 던지고, 관문 전이면 미뤘다가 관문이 열릴 때 로컬로 실패한다.
                    try {
                        tickets.add(endpoint.request(REQUEST_TYPE, new byte[70000], 1 + random.nextInt(60)));
                    } catch (IllegalArgumentException expected) {
                        trace.append("oversize\n");
                    }
                    break;
                default:
                    if (!sentStreams.isEmpty()) {
                        endpoint.receive(Frames.reset(streamOf(random, sentStreams), ErrorCode.REFUSED));
                    }
                    break;
            }
        }
        switch (random.nextInt(4)) {
            case 0:
                endpoint.close(ErrorCode.NO_ERROR);
                break;
            case 1:
                endpoint.close(ErrorCode.PROTOCOL_ERROR);
                break;
            case 2:
                endpoint.disconnect();
                break;
            default:
                break;
        }
        endpoint.advance(1000);
        append(trace, endpoint, tickets, sentStreams);
        return trace.toString();
    }

    private static List<String> digests() {
        List<String> digests = new ArrayList<>();
        for (long seed = 1; seed <= SCENARIOS; seed++) {
            digests.add(Golden.sha256(scenario(seed)));
        }
        return digests;
    }

    @Test
    void sameScenarioGivesSameTraceTwice() {
        assertThat(scenario(7)).isEqualTo(scenario(7)).contains("event");
    }

    @Test
    void scenariosCoverCancelAndExpiryOfEveryKind() {
        StringBuilder all = new StringBuilder();
        for (long seed = 1; seed <= SCENARIOS; seed++) {
            all.append(scenario(seed));
        }
        assertThat(all.toString())
                .contains("RESULT 0 " + Status.DEADLINE_EXCEEDED + " Optional[NOT_PROCESSED]")
                .contains("RESULT 1 " + Status.DEADLINE_EXCEEDED + " Optional[UNKNOWN]")
                .contains("RESULT 0 " + Status.CANCELLED + " Optional[NOT_PROCESSED]")
                .contains("RESULT 3 " + Status.CANCELLED)
                .contains("oversize")
                .containsPattern("RESULT [1-9][0-9]* " + Status.UNAVAILABLE + " Optional\\[NOT_PROCESSED\\]")
                .contains("state 0 QUEUED_WITHOUT_NUMBER")
                .contains("NUMBERED_UNWRITTEN")
                .contains("WRITING_STARTED");
    }

    @Test
    void cancelAndExpiryMatchGoldenMaster() {
        List<String> actual = digests();
        assertThat(actual).isEqualTo(Golden.lines(GOLDEN_RESOURCE, actual));
    }
}
