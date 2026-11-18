package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 요청 확장 영역(규격 2.4절)을 보내고 받는 경로와, 받은 요청을 핸들러 시작 전에 거절하는 경로(#316)를 본다. */
class EndpointRequestExtensionsTest {

    private static final int TYPE = 0x0100;
    private static final byte[] NO_BYTES = new byte[0];
    /** tag 0x0002(알 수 없는 선택 항목), 길이 3, 값 aa bb cc. */
    private static final byte[] EXTENSIONS = EndpointTest.bytes("00020003aabbcc");

    private static Endpoint establishedClient() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        client.receive(Frames.hello(EndpointConfig.defaults()));
        client.receive(Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        client.receive(Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));
        client.drainMessages();
        client.drainEvents();
        return client;
    }

    private static Endpoint establishedServer() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().knownTypes(Collections.singleton(TYPE)));
        server.receive(Frames.preface());
        server.receive(Frames.hello(EndpointConfig.defaults()));
        server.receive(Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        server.receive(Frames.auth("ANONYMOUS", NO_BYTES));
        server.drainMessages();
        server.drainEvents();
        return server;
    }

    @Test
    void requestCarriesExtensionsBeforeTheBodyInTheFrame() {
        Endpoint client = establishedClient();

        client.request(TYPE, EXTENSIONS, new byte[] {1, 2}, 0);

        List<OutboundMessage> sent = client.drainMessages();
        assertThat(sent).hasSize(1);
        byte[] frame = sent.get(0).bytes();
        assertThat(Frames.extensionsLengthOf(frame, 0)).isEqualTo(EXTENSIONS.length);
        byte[] expectedTail = new byte[EXTENSIONS.length + 2];
        System.arraycopy(EXTENSIONS, 0, expectedTail, 0, EXTENSIONS.length);
        expectedTail[EXTENSIONS.length] = 1;
        expectedTail[EXTENSIONS.length + 1] = 2;
        assertThat(java.util.Arrays.copyOfRange(frame, Frames.HEADER_SIZE, frame.length))
                .isEqualTo(expectedTail);
        // 길이 필드는 확장 영역을 포함한 전체 길이를 센다.
        assertThat(Frames.readUnsignedInt(frame, 0)).isEqualTo(frame.length - Frames.LENGTH_FIELD_SIZE);
    }

    @Test
    void extensionsOfARequestQueuedBeforeTheAuthGateAreSentWhenTheGateOpens() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        client.receive(Frames.hello(EndpointConfig.defaults()));
        client.receive(Frames.control(FrameType.HELLO, Flags.ACK, NO_BYTES));
        client.drainMessages();

        client.request(TYPE, EXTENSIONS, new byte[] {9}, 0);
        client.receive(Frames.control(FrameType.AUTH, Flags.ACK, NO_BYTES));

        byte[] frame = lastMessage(client);
        assertThat(Frames.extensionsLengthOf(frame, 0)).isEqualTo(EXTENSIONS.length);
    }

    @Test
    void requestWithExtensionsStillHonoursItsDeadline() {
        Endpoint client = establishedClient();

        long ticket = client.request(TYPE, EXTENSIONS, NO_BYTES, 10);
        client.drainMessages();
        client.advance(11);

        List<Event> events = client.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).ticket().getAsLong()).isEqualTo(ticket);
        assertThat(events.get(0).status()).isEqualTo(Status.DEADLINE_EXCEEDED);
    }

    @Test
    void malformedExtensionsAreRejectedBeforeAnyStateChanges() {
        Endpoint client = establishedClient();

        assertThatThrownBy(() -> client.request(TYPE, new byte[] {0, 2, 0}, NO_BYTES, 0))
                .isInstanceOf(IllegalArgumentException.class);
        // 번호표도 쓰이지 않았다.
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
        // timeout(tag 1)은 4바이트여야 한다.
        byte[] badTimeout = EndpointTest.bytes("000100020001");

        assertThatThrownBy(() -> client.request(TYPE, badTimeout, NO_BYTES, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void receivedRequestExposesItsExtensionsAndBody() {
        Endpoint server = establishedServer();
        byte[] frame = Frames.request(TYPE, 1, EXTENSIONS, new byte[] {7, 8});

        server.receive(frame);

        List<Event> events = server.drainEvents();
        assertThat(events).hasSize(1);
        assertThat(events.get(0).extensions()).isEqualTo(EXTENSIONS);
        assertThat(events.get(0).body()).isEqualTo(new byte[] {7, 8});
    }

    @Test
    void receivedRequestWithoutExtensionsHasEmptyExtensions() {
        Endpoint server = establishedServer();

        server.receive(Frames.request(TYPE, 1, NO_BYTES));

        assertThat(server.drainEvents().get(0).extensions()).isEmpty();
    }

    @Test
    void refusingADeliveredRequestSendsResetRefusedAndFreesTheStream() {
        Endpoint server = establishedServer();
        server.receive(Frames.request(TYPE, 1, NO_BYTES));
        server.drainEvents();
        server.drainMessages();

        server.refuseRequest(1);

        List<OutboundMessage> sent = server.drainMessages();
        assertThat(sent).hasSize(1);
        assertThat(sent.get(0).bytes()).isEqualTo(Frames.reset(1, ErrorCode.REFUSED));
        // 스트림이 끝났으므로 같은 스트림에 응답해도 아무 일이 없다.
        server.respond(1, 0, NO_BYTES);
        assertThat(server.drainMessages()).isEmpty();
    }

    @Test
    void refusingAnUnknownStreamDoesNothing() {
        Endpoint server = establishedServer();

        server.refuseRequest(99);

        assertThat(server.drainMessages()).isEmpty();
    }

    private static byte[] lastMessage(Endpoint endpoint) {
        List<OutboundMessage> sent = endpoint.drainMessages();
        return sent.get(sent.size() - 1).bytes();
    }

    @Test
    void respondReportsWhetherTheResponseWasAccepted() {
        Endpoint server = establishedServer();
        server.receive(Frames.request(TYPE, 1, NO_BYTES));
        server.drainEvents();
        server.drainMessages();

        assertThat(server.respond(1, 0, NO_BYTES)).isTrue();
        assertThat(server.respond(1, 0, NO_BYTES)).isFalse();
        assertThat(server.respond(99, 0, NO_BYTES)).isFalse();
    }

    @Test
    void respondToARequestThePeerResetIsNotAccepted() {
        Endpoint server = establishedServer();
        server.receive(Frames.request(TYPE, 1, NO_BYTES));
        server.drainEvents();
        server.receive(Frames.reset(1, ErrorCode.CANCEL));
        server.drainMessages();

        assertThat(server.respond(1, 0, NO_BYTES)).isFalse();
        assertThat(server.drainMessages()).isEmpty();
    }
}
