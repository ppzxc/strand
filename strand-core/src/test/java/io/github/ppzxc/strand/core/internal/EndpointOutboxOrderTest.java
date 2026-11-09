package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * 쓰기 대기열의 순서와 내용을 골든 마스터로 고정한다(#200). 고정 시드로 만든 무작위 연산 열(요청, 취소, PING, 응답, 상대 요청과 RESET, 시계,
 * 닫기, 중간 drain)을 {@code Endpoint}의 공개 입출력으로만 돌리고, 꺼낸 메시지와 이벤트의 SHA-256만 비교한다. 골든은 자료구조를 바꾸기 전
 * 구현이 낸 값이라 이 테스트가 통과하면 바꾼 뒤에도 송신 순서가 바이트 단위로 같다.
 *
 * <p>골든을 다시 만드는 방법은 {@link Golden}을 본다.
 */
class EndpointOutboxOrderTest {

    private static final String GOLDEN_RESOURCE = "outbox-order-golden.txt";
    private static final int SCENARIOS = 400;
    private static final int OPERATIONS = 45;
    private static final int REQUEST_TYPE = 0x0100;

    /** 시나리오가 도는 환경. 시드로 고른다. */
    private enum Variant {
        /** 바이트 스트림, 규격 기본값. */
        STREAM,
        /** 바이트 스트림, 짧은 확인 대기와 유휴 간격. drain이 시작한 확인 대기가 실제로 만료된다. */
        SHORT_TIMERS,
        /** WebSocket. 닫을 때 WebSocket Close가 맨 끝에 붙는다. */
        WEBSOCKET;

        EndpointConfig config() {
            switch (this) {
                case SHORT_TIMERS:
                    return EndpointConfig.defaults().ackTimeoutMs(100).idleIntervalMs(40);
                case WEBSOCKET:
                    return EndpointConfig.defaults().webSocket();
                default:
                    return EndpointConfig.defaults();
            }
        }
    }

    /** 상대가 보낸 바이트(WebSocket이면 메시지 하나)를 넣는다. */
    private static void feed(Variant variant, Endpoint endpoint, byte[] bytes) {
        if (variant == Variant.WEBSOCKET) {
            endpoint.receiveMessage(WebSocketMessageKind.BINARY, bytes);
        } else {
            endpoint.receive(bytes);
        }
    }

    /** 서버 쪽에서 서문, HELLO 교환, AUTH 통과까지 마친 엔드포인트. 상대가 알린 동시 스트림 수는 규격 기본값이다. */
    private static Endpoint established(Variant variant) {
        Endpoint server = Endpoint.server(variant.config());
        feed(variant, server, Frames.preface());
        feed(variant, server, Frames.hello(EndpointConfig.defaults()));
        feed(variant, server, Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        feed(variant, server, Frames.auth("ANONYMOUS", new byte[0]));
        server.drainMessages();
        server.drainEvents();
        return server;
    }

    private static void append(StringBuilder trace, Endpoint endpoint) {
        for (Event event : endpoint.drainEvents()) {
            trace.append("event ")
                    .append(event.kind())
                    .append(' ')
                    .append(event.streamId())
                    .append(' ')
                    .append(event.status())
                    .append(' ')
                    .append(event.type())
                    .append('\n');
        }
        for (OutboundMessage message : endpoint.drainMessages()) {
            trace.append("message ")
                    .append(message.kind())
                    .append(' ')
                    .append(Arrays.toString(message.bytes()))
                    .append('\n');
        }
    }

    private static byte[] payload(Random random) {
        byte[] bytes = new byte[random.nextInt(40)];
        random.nextBytes(bytes);
        return bytes;
    }

    private static String scenario(long seed) {
        Random random = new Random(seed);
        Variant variant = Variant.values()[(int) (seed % Variant.values().length)];
        Endpoint endpoint = established(variant);
        // 이 걸음에서 NO_ERROR로 닫는다. 이후의 취소, 만료, RESET, PING이 CLOSE가 들어 있는 대기열에 들어간다.
        int closeAtStep = random.nextInt(OPERATIONS * 2);
        StringBuilder trace = new StringBuilder();
        List<Long> tickets = new ArrayList<>();
        long peerStream = 1;
        for (int step = 0; step < OPERATIONS; step++) {
            if (step == closeAtStep) {
                endpoint.close(ErrorCode.NO_ERROR);
            }
            switch (random.nextInt(12)) {
                case 0:
                case 1:
                    tickets.add(endpoint.request(REQUEST_TYPE, payload(random)));
                    break;
                case 2:
                    tickets.add(endpoint.request(REQUEST_TYPE, payload(random), 1 + random.nextInt(100)));
                    break;
                case 3:
                    if (!tickets.isEmpty()) {
                        endpoint.cancelRequest(tickets.get(random.nextInt(tickets.size())));
                    }
                    break;
                case 4:
                    endpoint.ping(new byte[] {1, 2, 3, 4, 5, 6, 7, (byte) random.nextInt(256)});
                    break;
                case 5:
                    feed(variant, endpoint, Frames.request(REQUEST_TYPE, peerStream, payload(random)));
                    peerStream += 2;
                    break;
                case 6:
                    endpoint.respond(1 + 2L * random.nextInt(8), 0, payload(random));
                    break;
                case 7:
                    feed(variant, endpoint, Frames.reset(2L * (1 + random.nextInt(8)), ErrorCode.REFUSED));
                    break;
                case 8:
                    endpoint.advance(random.nextInt(80));
                    break;
                case 9:
                    feed(variant, endpoint, Frames.control(FrameType.PING, 0, new byte[8]));
                    break;
                case 10:
                    append(trace, endpoint);
                    break;
                default:
                    endpoint.cancel(2L * (1 + random.nextInt(8)));
                    break;
            }
        }
        switch (random.nextInt(5)) {
            case 0:
                endpoint.close(ErrorCode.NO_ERROR);
                break;
            case 1:
                endpoint.close(ErrorCode.PROTOCOL_ERROR);
                break;
            case 2:
                feed(variant, endpoint, Frames.close(ErrorCode.NO_ERROR, 0));
                break;
            case 3:
                endpoint.disconnect();
                break;
            default:
                break;
        }
        // 닫은 뒤에도 보내고 받는다. 닫힘 처리 뒤의 순서도 고정한다.
        endpoint.ping(new byte[8]);
        endpoint.request(REQUEST_TYPE, payload(random));
        feed(variant, endpoint, Frames.control(FrameType.PING, 0, new byte[8]));
        append(trace, endpoint);
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
    void sameScenarioGivesSameBytesTwice() {
        assertThat(scenario(7)).isEqualTo(scenario(7)).contains("message");
    }

    @Test
    void scenariosCoverTheOrderingRules() {
        StringBuilder all = new StringBuilder();
        for (long seed = 1; seed <= SCENARIOS; seed++) {
            all.append(scenario(seed));
        }
        assertThat(all.toString())
                .contains("event REQUEST_RECEIVED")
                .contains("event RESULT")
                .contains("event CLOSED")
                .contains("message WEBSOCKET_CLOSE")
                .contains("message BINARY");
    }

    @Test
    void sendOrderMatchesGoldenMaster() {
        List<String> actual = digests();
        assertThat(actual).isEqualTo(Golden.lines(GOLDEN_RESOURCE, actual));
    }
}
