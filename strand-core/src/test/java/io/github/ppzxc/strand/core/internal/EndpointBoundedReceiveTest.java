package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.io.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;

class EndpointBoundedReceiveTest {
    @Test
    void borrowedPartialInputIsCopiedAndResumed() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        byte[] first = EndpointTest.bytes("c0de5354");
        assertThat(server.receiveBounded(first, 0, first.length, 1)).isEqualTo(4);
        java.util.Arrays.fill(first, (byte) 0);
        byte[] rest = EndpointTest.bytes("52300000" + "0000001000010000000000000000000000000000");
        assertThat(server.receiveBounded(rest, 0, rest.length, 1)).isEqualTo(4);
        assertThat(server.drainMessages()).hasSize(1);
        assertThat(server.receiveBounded(rest, 4, rest.length - 4, 1)).isEqualTo(20);
        assertThat(server.drainMessages()).hasSize(1);
    }

    @Test
    void resumesAtTheUnconsumedUnit() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        byte[] input = EndpointTest.bytes("c0de535452300000" + "0000001000010000000000000000000000000000");

        int first = server.receiveBounded(input, 0, input.length, 1);
        assertThat(first).isEqualTo(8);
        assertThat(server.drainMessages()).hasSize(1);
        int second = server.receiveBounded(input, first, input.length - first, 1);
        assertThat(second).isEqualTo(20);
        assertThat(server.drainMessages()).hasSize(1);
    }

    private static Endpoint openServer() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        server.receive(EndpointTest.bytes("c0de535452300000"));
        server.receive(EndpointTest.bytes("0000001000010000000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001000010100000000000000000000000000"));
        server.receive(EndpointTest.bytes("0000001a0005000000000000000000000000000009414e4f4e594d4f5553"));
        server.drainMessages();
        server.drainEvents();
        return server;
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.write(part, 0, part.length);
        }
        return out.toByteArray();
    }

    @Test
    void stopsBeforeAUnitWhoseEventSpaceIsNotReservedAndResumesAfterCollection() {
        Endpoint server = openServer();
        server.request(257, new byte[0]);
        server.request(257, new byte[0]);
        server.request(257, new byte[0]);
        server.drainMessages();
        byte[] request = Frames.request(257, 1, new byte[0]);
        byte[] input = concat(request, Frames.close(ErrorCode.NO_ERROR, 0));

        ReceiveProgress first = server.receiveBounded(input, 0, input.length, 10, CollectionSpace.of(5, 1000));

        assertThat(first.consumed()).isEqualTo(request.length);
        assertThat(first.collectionRequired()).isTrue();
        assertThat(server.drainEvents(1)).extracting(Event::kind).containsExactly(Event.Kind.REQUEST_RECEIVED);

        server.respond(1, 0, new byte[0]);
        ReceiveProgress second = server.receiveBounded(
                input, first.consumed(), input.length - first.consumed(), 10, CollectionSpace.of(5, 1000));

        assertThat(second.consumed()).isEqualTo(input.length - request.length);
        assertThat(second.collectionRequired()).isFalse();
        assertThat(server.drainEvents())
                .extracting(Event::kind)
                .containsExactly(Event.Kind.RESULT, Event.Kind.RESULT, Event.Kind.RESULT, Event.Kind.CLOSED);
    }

    @Test
    void stopsBeforeAUnitWhoseOutputSpaceIsNotReserved() {
        Endpoint server = openServer();
        byte[] ping = EndpointTest.bytes("00000018 0002 00 00 0000000000000000 0000 0000 0102030405060708");
        byte[] input = concat(ping, ping, ping);

        ReceiveProgress first = server.receiveBounded(input, 0, input.length, 10, CollectionSpace.of(100, 5));

        assertThat(first.consumed()).isEqualTo(2 * ping.length);
        assertThat(first.collectionRequired()).isTrue();
        assertThat(server.drainMessages()).hasSize(2);
        ReceiveProgress second = server.receiveBounded(
                input, first.consumed(), input.length - first.consumed(), 10, CollectionSpace.of(100, 5));
        assertThat(second.consumed()).isEqualTo(ping.length);
        assertThat(second.collectionRequired()).isFalse();
    }

    @Test
    void rejectsSpaceThatCannotHoldEvenOneUnitWhenNothingIsWaitingForCollection() {
        Endpoint server = openServer();
        byte[] input = Frames.request(257, 1, new byte[0]);

        assertThatIllegalStateException()
                .isThrownBy(() -> server.receiveBounded(input, 0, input.length, 1, CollectionSpace.of(1, 1)));
    }
}
