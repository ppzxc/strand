package io.github.ppzxc.strand.core.internal;

import java.util.ArrayList;
import java.util.List;

final class BenchSupport {

    static final int REQUEST_TYPE = 0x0100;

    static final int RESERVED_TYPE = 0x0006;

    private BenchSupport() {}

    static Endpoint[] establish(EndpointConfig config) {
        Endpoint client = Endpoint.client(config);
        Endpoint server = Endpoint.server(config);
        for (int round = 0; round < 6; round++) {
            pump(client, server);
            answerAuthentication(server);
            pump(server, client);
        }
        if (!drainEvents(client).stream().anyMatch(e -> e.kind() == Event.Kind.AUTHENTICATED)) {
            throw new IllegalStateException("클라이언트가 AUTH 관문을 통과하지 못했다");
        }
        drainEvents(server);
        return new Endpoint[] {client, server};
    }

    static void pump(Endpoint from, Endpoint to) {
        for (OutboundMessage message : drainMessages(from)) {
            receive(to, message.bytes());
        }
    }

    static void answerAuthentication(Endpoint server) {
        for (Event event : drainEvents(server)) {
            if (event.kind() == Event.Kind.AUTHENTICATION_REQUIRED) {
                server.completeAuthentication(event.authenticationId(), AuthDecision.PASS);
            }
        }
    }

    static void receive(Endpoint endpoint, byte[] bytes) {
        int offset = 0;
        while (offset < bytes.length) {
            ReceiveProgress progress = endpoint.receiveBounded(
                    bytes,
                    offset,
                    bytes.length - offset,
                    Integer.MAX_VALUE,
                    CollectionSpace.of(Integer.MAX_VALUE, Integer.MAX_VALUE));
            if (progress.consumed() == 0) {
                return;
            }
            offset += progress.consumed();
        }
    }

    static List<OutboundMessage> drainMessages(Endpoint endpoint) {
        List<OutboundMessage> drained = new ArrayList<>();
        EndpointOutput output;
        while ((output = endpoint.nextOutput()) != null) {
            drained.add(output.message());
            endpoint.outputProgress(output.id(), output.length());
        }
        return drained;
    }

    static List<Event> drainEvents(Endpoint endpoint) {
        return endpoint.drainEvents(Integer.MAX_VALUE);
    }

    static long[] receivedStreamIds(Endpoint server, ResponseOrder order) {
        List<Long> ids = new ArrayList<>();
        for (Event event : drainEvents(server)) {
            if (event.kind() == Event.Kind.REQUEST_RECEIVED) {
                ids.add(event.streamId());
            }
        }
        long[] result = new long[ids.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = ids.get(order == ResponseOrder.OLDEST_FIRST ? i : result.length - 1 - i);
        }
        return result;
    }

    static void receiveAll(Endpoint to, List<OutboundMessage> messages) {
        for (OutboundMessage message : messages) {
            receive(to, message.bytes());
        }
    }

    static long countEvents(Endpoint endpoint, Event.Kind kind) {
        return drainEvents(endpoint).stream().filter(e -> e.kind() == kind).count();
    }

    static void expectCount(String what, long expected, long actual) {
        if (expected != actual) {
            throw new IllegalStateException(what + "이(가) " + expected + "개가 아니라 " + actual + "개다");
        }
    }

    static Endpoint[] establishWithPendingRequests(int count) {
        Endpoint[] pair = establish(EndpointConfig.defaults().maxConcurrentStreams(count));
        for (int i = 0; i < count; i++) {
            pair[0].request(REQUEST_TYPE, new byte[0]);
        }
        pump(pair[0], pair[1]);
        return pair;
    }

    static final class Clock {
        private long elapsedNanos;

        void advance(Endpoint endpoint, long ms) {
            elapsedNanos += ms * Deadlines.NANOS_PER_MILLI;
            endpoint.advanceTo(elapsedNanos);
        }
    }

    static byte[] reservedFrame(int size) {
        return Frames.control(RESERVED_TYPE, 0, new byte[size - Frames.HEADER_SIZE]);
    }
}
