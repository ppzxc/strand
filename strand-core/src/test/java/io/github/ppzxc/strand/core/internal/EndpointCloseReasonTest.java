package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

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
        Endpoint server = Endpoint.server(EndpointConfig.defaults());
        server.receive(bytes(PREFACE));
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.receive(bytes(AUTH_ANONYMOUS));
        server.drainMessages();
        server.drainEvents();
        return server;
    }

    private static Endpoint openClient() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        client.receive(bytes(AUTH_ACK));
        client.drainMessages();
        client.drainEvents();
        return client;
    }

    private static Event closedEvent(Endpoint endpoint) {
        List<Event> events = endpoint.drainEvents();
        Event closed = events.get(events.size() - 1);
        assertThat(closed.kind()).isEqualTo(Event.Kind.CLOSED);
        return closed;
    }

    private static CloseReason closedReason(Endpoint endpoint) {
        return closedEvent(endpoint).closeReason().orElseThrow(AssertionError::new);
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

        server.receive(Frames.close(ErrorCode.NO_ERROR, 0));

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

        server.receive(Frames.close(ErrorCode.EXCESSIVE_USE, 0));

        assertErrorClose(closedReason(server), ErrorCode.EXCESSIVE_USE, CloseReason.Sender.PEER);
    }

    @Test
    void peerErrorCloseDuringClosingReplacesTheNormalClose() {
        Endpoint client = openClient();
        client.request(257, new byte[0]);
        client.close(ErrorCode.NO_ERROR);
        client.drainEvents();

        client.receive(Frames.close(ErrorCode.INTERNAL_ERROR, 0));

        assertErrorClose(closedReason(client), ErrorCode.INTERNAL_ERROR, CloseReason.Sender.PEER);
    }

    @Test
    void connectionErrorEndsAsProtocolErrorCloseSentByMe() {
        Endpoint server = openServer();

        server.receive(bytes(BAD_FRAME));

        assertErrorClose(closedReason(server), ErrorCode.PROTOCOL_ERROR, CloseReason.Sender.LOCAL);
    }

    @Test
    void connectionErrorWithoutAnySentCloseStillReportsAnErrorClose() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults());

        server.receive(bytes("0000000000000000"));

        CloseReason reason = closedReason(server);
        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
        assertThat(reason.errorCode()).isEqualTo(ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    void authenticatorRejectionEndsAsErrorCloseWithoutALocalCause() {
        Endpoint server = Endpoint.server(EndpointConfig.defaults().authenticator((m, c) -> AuthDecision.REJECT));
        server.receive(bytes(PREFACE));
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.receive(bytes(AUTH_ANONYMOUS));

        Event closed = closedEvent(server);

        assertErrorClose(closed.closeReason().get(), ErrorCode.UNAUTHENTICATED, CloseReason.Sender.LOCAL);
        assertThat(closed.cause()).isEmpty();
    }

    @Test
    void authenticatorExceptionIsStillReportedAlongsideTheErrorClose() {
        IllegalStateException failure = new IllegalStateException("auth backend down");
        Endpoint server = Endpoint.server(EndpointConfig.defaults().authenticator((m, c) -> {
            throw failure;
        }));
        server.receive(bytes(PREFACE));
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        server.receive(bytes(AUTH_ANONYMOUS));

        Event closed = closedEvent(server);

        assertThat(closed.cause()).containsSame(failure);
        assertErrorClose(closed.closeReason().get(), ErrorCode.INTERNAL_ERROR, CloseReason.Sender.LOCAL);
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
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));

        client.disconnect();

        assertThat(closedReason(client).kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
    }

    @Test
    void disconnectWhileClosingEndsAsDisconnected() {
        Endpoint client = openClient();
        client.request(257, new byte[0]);
        client.close(ErrorCode.NO_ERROR);
        client.drainEvents();

        client.disconnect();

        assertThat(closedReason(client).kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
    }

    @Test
    void otherEventsCarryNoCloseReason() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        client.receive(bytes(AUTH_ACK));

        for (Event event : client.drainEvents()) {
            assertThat(event.closeReason()).isEmpty();
        }
    }
}
