package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

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

    private static void append(StringBuilder trace, Endpoint endpoint, List<Long> tickets, List<Long> sentStreams) {
        states(trace, endpoint, tickets);
        for (Event event : Drive.drainEvents(endpoint)) {
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
        for (OutboundMessage message : Drive.drainMessages(endpoint)) {
            trace.append("message ").append(Arrays.toString(message.bytes())).append('\n');
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
        Drive.receive(endpoint, Frames.hello(EndpointConfig.defaults()));
        Drive.receive(endpoint, Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        Drive.drainMessages(endpoint);

        int authAtStep = random.nextInt(OPERATIONS);

        int expireAndCloseAtStep = random.nextInt(OPERATIONS * 3);
        StringBuilder trace = new StringBuilder();
        List<Long> tickets = new ArrayList<>();
        List<Long> sentStreams = new ArrayList<>();
        for (int step = 0; step < OPERATIONS; step++) {
            if (step == authAtStep) {
                Drive.receive(endpoint, Frames.control(FrameType.AUTH, Flags.ACK, new byte[0]));
            }
            if (step == expireAndCloseAtStep) {
                Drive.advance(endpoint, 70);
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
                        Drive.receive(
                                endpoint, Frames.response(REQUEST_TYPE, streamOf(random, sentStreams), 0, new byte[0]));
                    }
                    break;
                case 7:
                    Drive.advance(endpoint, random.nextInt(random.nextBoolean() ? 20 : 150));
                    break;
                case 8:
                case 11:
                    append(trace, endpoint, tickets, sentStreams);
                    break;
                case 9:
                    Drive.receive(endpoint, Frames.close(ErrorCode.NO_ERROR, streamOf(random, sentStreams)));
                    break;
                case 10:
                    try {
                        tickets.add(endpoint.request(REQUEST_TYPE, new byte[70000], 1 + random.nextInt(60)));
                    } catch (IllegalArgumentException expected) {
                        trace.append("oversize\n");
                    }
                    break;
                default:
                    if (!sentStreams.isEmpty()) {
                        Drive.receive(endpoint, Frames.reset(streamOf(random, sentStreams), ErrorCode.REFUSED));
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
        Drive.advance(endpoint, 1000);
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
