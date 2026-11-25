package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class EndpointRequestExtensionsTest {

    private static final int TYPE = 0x0100;
    private static final byte[] NO_BYTES = new byte[0];

    private static final byte[] EXTENSIONS = EndpointTest.bytes("00020003aabbcc");

    private static Endpoint establishedClient() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Drive.receive(client, Frames.hello(EndpointConfig.defaults()));
        Drive.receive(client, Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        Drive.receive(client, Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));
        Drive.drainMessages(client);
        Drive.drainEvents(client);
        return client;
    }

    private static Endpoint establishedServer() {
        Endpoint server = Drive.server(EndpointConfig.defaults().knownTypes(Collections.singleton(TYPE)));
        Drive.receive(server, Frames.preface());
        Drive.receive(server, Frames.hello(EndpointConfig.defaults()));
        Drive.receive(server, Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        Drive.receive(server, Frames.auth("ANONYMOUS", NO_BYTES));
        Drive.drainMessages(server);
        Drive.drainEvents(server);
        return server;
    }

    @Test
    void requestCarriesExtensionsBeforeTheBodyInTheFrame() {
        Endpoint client = establishedClient();

        client.request(TYPE, EXTENSIONS, new byte[] {1, 2}, 0);

        List<OutboundMessage> sent = Drive.drainMessages(client);
        assertThat(sent).hasSize(1);
        byte[] frame = sent.get(0).bytes();
        assertThat(Frames.extensionsLengthOf(frame, 0)).isEqualTo(EXTENSIONS.length);
        byte[] expectedTail = new byte[EXTENSIONS.length + 2];
        System.arraycopy(EXTENSIONS, 0, expectedTail, 0, EXTENSIONS.length);
        expectedTail[EXTENSIONS.length] = 1;
        expectedTail[EXTENSIONS.length + 1] = 2;
        assertThat(java.util.Arrays.copyOfRange(frame, Frames.HEADER_SIZE, frame.length))
                .isEqualTo(expectedTail);

        assertThat(Frames.readUnsignedInt(frame, 0)).isEqualTo(frame.length - Frames.LENGTH_FIELD_SIZE);
    }

    @Test
    void extensionsOfARequestQueuedBeforeTheAuthGateAreSentWhenTheGateOpens() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Drive.receive(client, Frames.hello(EndpointConfig.defaults()));
        Drive.receive(client, Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        Drive.drainMessages(client);

        client.request(TYPE, EXTENSIONS, new byte[] {9}, 0);
        Drive.receive(client, Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));

        byte[] frame = lastMessage(client);
        assertThat(Frames.extensionsLengthOf(frame, 0)).isEqualTo(EXTENSIONS.length);
    }

    @Test
    void requestWithExtensionsStillHonoursItsDeadline() {
        Endpoint client = establishedClient();

        long ticket = client.request(TYPE, EXTENSIONS, NO_BYTES, 10);
        Drive.drainMessages(client);
        Drive.advance(client, 11);

        List<Event> events = Drive.drainEvents(client);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).ticket().getAsLong()).isEqualTo(ticket);
        assertThat(events.get(0).status()).isEqualTo(Status.DEADLINE_EXCEEDED);
    }

    @Test
    void malformedExtensionsAreRejectedBeforeAnyStateChanges() {
        Endpoint client = establishedClient();

        assertThatThrownBy(() -> client.request(TYPE, new byte[] {0, 2, 0}, NO_BYTES, 0))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(client.request(TYPE, NO_BYTES, NO_BYTES, 0)).isZero();
    }

    @Test
    void oversizedExtensionsAreRejected() {
        Endpoint client = establishedClient();
        byte[] big = new byte[4 + 8189];
        big[2] = (byte) (8189 >> 8);
        big[3] = (byte) 8189;

        assertThatThrownBy(() -> client.request(TYPE, big, NO_BYTES, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidKnownExtensionValueIsRejected() {
        Endpoint client = establishedClient();

        byte[] badTimeout = EndpointTest.bytes("000100020001");

        assertThatThrownBy(() -> client.request(TYPE, badTimeout, NO_BYTES, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void receivedRequestExposesItsExtensionsAndBody() {
        Endpoint server = establishedServer();
        byte[] frame = Frames.request(TYPE, 1, EXTENSIONS, new byte[] {7, 8});

        Drive.receive(server, frame);

        List<Event> events = Drive.drainEvents(server);
        assertThat(events).hasSize(1);
        assertThat(events.get(0).extensions()).isEqualTo(EXTENSIONS);
        assertThat(events.get(0).body()).isEqualTo(new byte[] {7, 8});
    }

    @Test
    void receivedRequestWithoutExtensionsHasEmptyExtensions() {
        Endpoint server = establishedServer();

        Drive.receive(server, Frames.request(TYPE, 1, NO_BYTES));

        assertThat(Drive.drainEvents(server).get(0).extensions()).isEmpty();
    }

    @Test
    void refusingADeliveredRequestSendsResetRefusedAndFreesTheStream() {
        Endpoint server = establishedServer();
        Drive.receive(server, Frames.request(TYPE, 1, NO_BYTES));
        Drive.drainEvents(server);
        Drive.drainMessages(server);

        server.refuseRequest(1);

        List<OutboundMessage> sent = Drive.drainMessages(server);
        assertThat(sent).hasSize(1);
        assertThat(sent.get(0).bytes()).isEqualTo(Frames.reset(1, ErrorCode.REFUSED));

        server.respond(1, 0, NO_BYTES);
        assertThat(Drive.drainMessages(server)).isEmpty();
    }

    @Test
    void refusingAnUnknownStreamDoesNothing() {
        Endpoint server = establishedServer();

        server.refuseRequest(99);

        assertThat(Drive.drainMessages(server)).isEmpty();
    }

    private static byte[] lastMessage(Endpoint endpoint) {
        List<OutboundMessage> sent = Drive.drainMessages(endpoint);
        return sent.get(sent.size() - 1).bytes();
    }

    @Test
    void respondReportsWhetherTheResponseWasAccepted() {
        Endpoint server = establishedServer();
        Drive.receive(server, Frames.request(TYPE, 1, NO_BYTES));
        Drive.drainEvents(server);
        Drive.drainMessages(server);

        assertThat(server.respond(1, 0, NO_BYTES)).isTrue();
        assertThat(server.respond(1, 0, NO_BYTES)).isFalse();
        assertThat(server.respond(99, 0, NO_BYTES)).isFalse();
    }

    @Test
    void respondToARequestThePeerResetIsNotAccepted() {
        Endpoint server = establishedServer();
        Drive.receive(server, Frames.request(TYPE, 1, NO_BYTES));
        Drive.drainEvents(server);
        Drive.receive(server, Frames.reset(1, ErrorCode.CANCEL));
        Drive.drainMessages(server);

        assertThat(server.respond(1, 0, NO_BYTES)).isFalse();
        assertThat(Drive.drainMessages(server)).isEmpty();
    }
}
