package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class EndpointOutboxOrderTest {

    private static final String GOLDEN_RESOURCE = "outbox-order-golden.txt";
    private static final int SCENARIOS = 400;
    private static final int OPERATIONS = 45;
    private static final int REQUEST_TYPE = 0x0100;

    private enum Variant {
        STREAM,

        SHORT_TIMERS,

        UNIT;

        EndpointConfig config() {
            switch (this) {
                case SHORT_TIMERS:
                    return EndpointConfig.defaults().ackTimeoutMs(100).idleIntervalMs(40);
                default:
                    return EndpointConfig.defaults();
            }
        }
    }

    private static void feed(Variant variant, Endpoint endpoint, byte[] bytes) {
        if (variant == Variant.UNIT) {
            endpoint.receiveUnit(bytes, 0, bytes.length, EndpointDriver.ROOMY);
            Drive.settle(endpoint);
        } else {
            Drive.receive(endpoint, bytes);
        }
    }

    private static Endpoint established(Variant variant) {
        Endpoint server = Drive.server(variant.config());
        feed(variant, server, Frames.preface());
        feed(variant, server, Frames.hello(EndpointConfig.defaults()));
        feed(variant, server, Frames.control(FrameType.HELLO, Flags.ACK, new byte[0]));
        feed(variant, server, Frames.auth("ANONYMOUS", new byte[0]));
        Drive.drainMessages(server);
        Drive.drainEvents(server);
        return server;
    }

    private static void append(StringBuilder trace, Endpoint endpoint) {
        for (Event event : Drive.drainEvents(endpoint)) {
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
        for (OutboundMessage message : Drive.drainMessages(endpoint)) {
            trace.append("message ").append(Arrays.toString(message.bytes())).append('\n');
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
                    Drive.advance(endpoint, random.nextInt(80));
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
                .contains("message [");
    }

    @Test
    void sendOrderMatchesGoldenMaster() {
        List<String> actual = digests();
        assertThat(actual).isEqualTo(Golden.lines(GOLDEN_RESOURCE, actual));
    }
}
