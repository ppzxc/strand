package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

/** {@code CLOSED} 이벤트가 싣는 종료 원인: 끝난 방식(CLOSE, 오류 CLOSE, 끊김), error_code, CLOSE를 보낸 쪽. */
class EndpointCloseReasonTest {

    private static final String HELLO = "0000001000010000000000000000000000000000";
    private static final String HELLO_ACK = "0000001000010100000000000000000000000000";
    private static final String AUTH_ACK = "0000001000050100000000000000000000000000";
    private static final String AUTH_ANONYMOUS = "0000001a0005000000000000000000000000000009414e4f4e594d4f5553";
    private static final String PREFACE = "c0de535452300000";
    private static final String BAD_FRAME = "0000000f0004" + "00000000000000000000000000";

    private static byte[] bytes(String hex) {
        return EndpointTest.bytes(hex);
    }

    private static Endpoint openServer() {
        Endpoint server = Drive.server(EndpointConfig.defaults());
        Drive.receive(server, bytes(PREFACE));
        Drive.receive(server, bytes(HELLO));
        Drive.receive(server, bytes(HELLO_ACK));
        Drive.receive(server, bytes(AUTH_ANONYMOUS));
        Drive.drainMessages(server);
        Drive.drainEvents(server);
        return server;
    }

    private static Endpoint openClient() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Drive.receive(client, bytes(HELLO));
        Drive.receive(client, bytes(HELLO_ACK));
        Drive.receive(client, bytes(AUTH_ACK));
        Drive.drainMessages(client);
        Drive.drainEvents(client);
        return client;
    }

    private static Event closedEvent(Endpoint endpoint) {
        List<Event> events = Drive.drainEvents(endpoint);
        Event closed = events.get(events.size() - 1);
        assertThat(closed.kind()).isEqualTo(Event.Kind.CLOSED);
        return closed;
    }

    private static CloseReason closedReason(Endpoint endpoint) {
        return closedEvent(endpoint).closeReason().orElseThrow(AssertionError::new);
    }

    @Test
    void applicationCanAttachUtf8ReasonToClose() {
        Endpoint server = openServer();

        server.close(ErrorCode.NO_ERROR, "hi");

        assertThat(EndpointTest.hex(Drive.drainMessages(server).get(0).bytes()))
                .isEqualTo("0000001e000300000000000000000000000000000000000000000000000000006869");
    }

    @Test
    void closeRejectsANullReasonBeforeChangingConnectionState() {
        Endpoint server = openServer();

        assertThatThrownBy(() -> server.close(ErrorCode.NO_ERROR, null)).isInstanceOf(NullPointerException.class);
        assertThat(Drive.drainMessages(server)).isEmpty();
        assertThat(Drive.drainEvents(server)).isEmpty();
    }

    @Test
    void closeRejectsANullReasonEvenAfterClosingStarted() {
        Endpoint server = openServer();
        server.close(ErrorCode.NO_ERROR, "first");
        Drive.drainMessages(server);
        Drive.drainEvents(server);

        assertThatThrownBy(() -> server.close(ErrorCode.NO_ERROR, null)).isInstanceOf(NullPointerException.class);
        assertThat(Drive.drainMessages(server)).isEmpty();
        assertThat(Drive.drainEvents(server)).isEmpty();
    }

    @Test
    void closeReasonIsTruncatedAtTheFrameLimitOnAValidUtf8Boundary() {
        Endpoint server = openServer();
        StringBuilder reason = new StringBuilder();
        for (int i = 0; i < 21_836; i++) reason.append("가");

        server.close(ErrorCode.NO_ERROR, reason.toString());

        byte[] frame = Drive.drainMessages(server).get(0).bytes();
        assertThat(frame).hasSize(65_534);
        assertThat(EndpointTest.hex(java.util.Arrays.copyOfRange(frame, frame.length - 3, frame.length)))
                .isEqualTo("eab080");
    }

    @Test
    void closeReasonPreservesValuesBelowAndExactlyAtTheNegotiatedLimit() {
        for (int reasonBytes : new int[] {65_503, 65_504}) {
            Endpoint server = openServer();
            StringBuilder reason = new StringBuilder();
            for (int i = 0; i < reasonBytes; i++) reason.append('a');

            server.close(ErrorCode.NO_ERROR, reason.toString());

            assertThat(Drive.drainMessages(server).get(0).bytes()).hasSize(32 + reasonBytes);
        }
    }

    @Test
    void closeBeforePeerHelloUsesTheSixteenKiBFrameLimit() {
        Endpoint server = Drive.server(EndpointConfig.defaults());
        Drive.receive(server, bytes(PREFACE));
        Drive.drainMessages(server);
        StringBuilder reason = new StringBuilder();
        for (int i = 0; i < 20_000; i++) reason.append('x');

        server.close(ErrorCode.NO_ERROR, reason.toString());

        assertThat(Drive.drainMessages(server).get(0).bytes()).hasSize(16_384);
    }

    private static void assertErrorClose(CloseReason reason, int errorCode, CloseReason.Sender sender) {
        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
        assertThat(reason.errorCode()).isEqualTo(errorCode);
        assertThat(reason.sender()).contains(sender);
    }

    @Test
    void localNoErrorCloseEndsAsCloseSentByMe() {
        Endpoint server = openServer();

        server.close(ErrorCode.NO_ERROR);

        CloseReason reason = closedReason(server);
        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.CLOSE);
        assertThat(reason.errorCode()).isEqualTo(ErrorCode.NO_ERROR);
        assertThat(reason.sender()).contains(CloseReason.Sender.LOCAL);
    }

    @Test
    void peerNoErrorCloseEndsAsCloseSentByThePeer() {
        Endpoint server = openServer();

        Drive.receive(server, Frames.close(ErrorCode.NO_ERROR, 0));

        CloseReason reason = closedReason(server);
        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.CLOSE);
        assertThat(reason.sender()).contains(CloseReason.Sender.PEER);
    }

    @Test
    void closeAfterStreamsDrainKeepsTheSenderOfTheFirstClose() {
        Endpoint client = openClient();
        client.request(257, new byte[0]);
        client.close(ErrorCode.NO_ERROR);
        client.abortClosing();

        CloseReason reason = closedReason(client);

        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.CLOSE);
        assertThat(reason.sender()).contains(CloseReason.Sender.LOCAL);
    }

    @Test
    void localErrorCloseCarriesItsErrorCode() {
        Endpoint server = openServer();

        server.close(ErrorCode.UNRESPONSIVE);

        assertErrorClose(closedReason(server), ErrorCode.UNRESPONSIVE, CloseReason.Sender.LOCAL);
    }

    @Test
    void peerErrorCloseCarriesItsErrorCode() {
        Endpoint server = openServer();

        Drive.receive(server, Frames.close(ErrorCode.EXCESSIVE_USE, 0));

        assertErrorClose(closedReason(server), ErrorCode.EXCESSIVE_USE, CloseReason.Sender.PEER);
    }

    @Test
    void peerErrorCloseDuringClosingReplacesTheNormalClose() {
        Endpoint client = openClient();
        client.request(257, new byte[0]);
        client.close(ErrorCode.NO_ERROR);
        Drive.drainEvents(client);

        Drive.receive(client, Frames.close(ErrorCode.INTERNAL_ERROR, 0));

        assertErrorClose(closedReason(client), ErrorCode.INTERNAL_ERROR, CloseReason.Sender.PEER);
    }

    @Test
    void connectionErrorEndsAsProtocolErrorCloseSentByMe() {
        Endpoint server = openServer();

        Drive.receive(server, bytes(BAD_FRAME));

        assertErrorClose(closedReason(server), ErrorCode.PROTOCOL_ERROR, CloseReason.Sender.LOCAL);
    }

    @Test
    void connectionErrorWithoutAnySentCloseStillReportsAnErrorClose() {
        Endpoint server = Drive.server(EndpointConfig.defaults());

        Drive.receive(server, bytes("0000000000000000"));

        CloseReason reason = closedReason(server);
        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
        assertThat(reason.errorCode()).isEqualTo(ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    void authenticatorRejectionEndsAsErrorCloseWithoutALocalCause() {
        Endpoint server = Drive.server(EndpointConfig.defaults(), (m, c) -> AuthDecision.REJECT);
        Drive.receive(server, bytes(PREFACE));
        Drive.receive(server, bytes(HELLO));
        Drive.receive(server, bytes(HELLO_ACK));
        Drive.receive(server, bytes(AUTH_ANONYMOUS));

        Event closed = closedEvent(server);

        assertErrorClose(closed.closeReason().get(), ErrorCode.UNAUTHENTICATED, CloseReason.Sender.LOCAL);
        assertThat(closed.cause()).isEmpty();
    }

    @Test
    void disconnectWithoutCloseEndsAsDisconnected() {
        Endpoint server = openServer();

        server.disconnect();

        CloseReason reason = closedReason(server);
        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
        assertThat(reason.sender()).isEmpty();
    }

    @Test
    void disconnectBeforeTheAuthGateEndsAsDisconnected() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Drive.receive(client, bytes(HELLO));
        Drive.receive(client, bytes(HELLO_ACK));

        client.disconnect();

        assertThat(closedReason(client).kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
    }

    @Test
    void disconnectWhileClosingEndsAsDisconnected() {
        Endpoint client = openClient();
        client.request(257, new byte[0]);
        client.close(ErrorCode.NO_ERROR);
        Drive.drainEvents(client);

        client.disconnect();

        assertThat(closedReason(client).kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
    }

    @Test
    void ackTimeoutWhileClosingReportsTheActualErrorCodeSentByMe() {
        EndpointConfig config = EndpointConfig.defaults().ackTimeoutMs(1000).idleIntervalMs(500);
        Endpoint server = Drive.server(config);
        Drive.receive(server, bytes(PREFACE));
        Drive.receive(server, bytes(HELLO));
        Drive.receive(server, bytes(HELLO_ACK));
        Drive.receive(server, bytes(AUTH_ANONYMOUS));
        Drive.receive(server, Frames.request(257, 1, new byte[0]));
        Drive.advance(server, 600);
        Drive.drainMessages(server);
        server.close(ErrorCode.NO_ERROR);
        Drive.drainEvents(server);

        Drive.advance(server, 1500);

        assertErrorClose(closedReason(server), ErrorCode.UNRESPONSIVE, CloseReason.Sender.LOCAL);
    }

    @Test
    void ackTimeoutAfterThePeerClosedReportsTheActualErrorCode() {
        EndpointConfig config = EndpointConfig.defaults().ackTimeoutMs(1000).idleIntervalMs(500);
        Endpoint server = Drive.server(config);
        Drive.receive(server, bytes(PREFACE));
        Drive.receive(server, bytes(HELLO));
        Drive.receive(server, bytes(HELLO_ACK));
        Drive.receive(server, bytes(AUTH_ANONYMOUS));
        Drive.receive(server, Frames.request(257, 1, new byte[0]));
        Drive.advance(server, 600);
        Drive.drainMessages(server);
        Drive.receive(server, Frames.close(ErrorCode.NO_ERROR, 0));
        Drive.drainEvents(server);

        Drive.advance(server, 1500);

        assertErrorClose(closedReason(server), ErrorCode.UNRESPONSIVE, CloseReason.Sender.LOCAL);
    }

    @Test
    void otherEventsCarryNoCloseReason() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        Drive.receive(client, bytes(HELLO));
        Drive.receive(client, bytes(HELLO_ACK));
        Drive.receive(client, bytes(AUTH_ACK));

        for (Event event : Drive.drainEvents(client)) {
            assertThat(event.closeReason()).isEmpty();
        }
    }
}
