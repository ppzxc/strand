package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * 끝난 연결(전송 계층이 닫힌 연결)에서 각 공개 메서드가 하는 일. 끝난 연결은 입력과 시간을 무시하고, 요청만 로컬 {@code UNAVAILABLE}로
 * 실패하며 번호표를 쓴다. 끝나는 길(끊김, 오류 CLOSE, {@code NO_ERROR} CLOSE)과 끝난 시점(AUTH 관문 전, 열림)이 달라도 같다.
 */
class EndpointEndedConnectionTest {

    private static final String HELLO = "0000001000010000000000000000000000000000";
    private static final String HELLO_ACK = "0000001000010100000000000000000000000000";
    private static final String AUTH_ACK = "0000001000050100000000000000000000000000";
    private static final String AUTH_ANONYMOUS = "0000001a0005000000000000000000000000000009414e4f4e594d4f5553";
    private static final String PREFACE = "c0de535452300000";
    private static final byte[] PING_PAYLOAD = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};

    private static byte[] bytes(String hex) {
        return EndpointTest.bytes(hex);
    }

    private static Endpoint openClient() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        client.receive(bytes(HELLO));
        client.receive(bytes(HELLO_ACK));
        client.receive(bytes(AUTH_ACK));
        return client;
    }

    private static Endpoint serverAwaitingAuth(EndpointConfig config) {
        Endpoint server = Endpoint.server(config);
        server.receive(bytes(PREFACE));
        server.receive(bytes(HELLO));
        server.receive(bytes(HELLO_ACK));
        return server;
    }

    private static Endpoint openServer(EndpointConfig config) {
        Endpoint server = serverAwaitingAuth(config);
        server.receive(bytes(AUTH_ANONYMOUS));
        return server;
    }

    private static Endpoint openServer() {
        return openServer(EndpointConfig.defaults());
    }

    private static Endpoint openWebSocketClient() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults().webSocket());
        client.receiveMessage(WebSocketMessageKind.BINARY, bytes(HELLO));
        client.receiveMessage(WebSocketMessageKind.BINARY, bytes(HELLO_ACK));
        client.receiveMessage(WebSocketMessageKind.BINARY, bytes(AUTH_ACK));
        return client;
    }

    private static Endpoint settle(Endpoint endpoint) {
        endpoint.drainMessages();
        endpoint.drainEvents();
        return endpoint;
    }

    /** 끝난 클라이언트 연결들. 끝난 길과 끝난 시점이 서로 다르다. */
    private static Map<String, Endpoint> endedClients() {
        Map<String, Endpoint> ended = new LinkedHashMap<>();
        Endpoint beforeHello = Endpoint.client(EndpointConfig.defaults());
        beforeHello.disconnect();
        ended.put("연결 직후 끊김", beforeHello);
        Endpoint beforeAuth = Endpoint.client(EndpointConfig.defaults());
        beforeAuth.receive(bytes(HELLO));
        beforeAuth.receive(bytes(HELLO_ACK));
        beforeAuth.disconnect();
        ended.put("AUTH 관문 전 끊김", beforeAuth);
        Endpoint disconnected = openClient();
        disconnected.disconnect();
        ended.put("열림에서 끊김", disconnected);
        Endpoint errorClosed = openClient();
        errorClosed.close(ErrorCode.PROTOCOL_ERROR);
        ended.put("오류 CLOSE로 닫음", errorClosed);
        Endpoint normallyClosed = openClient();
        normallyClosed.close(ErrorCode.NO_ERROR);
        ended.put("NO_ERROR CLOSE로 닫음", normallyClosed);
        Endpoint badFrame = openClient();
        badFrame.receive(bytes("0000000f0004" + "00000000000000000000000000"));
        ended.put("연결 오류로 닫음", badFrame);
        return ended;
    }

    /** 끝난 서버 연결들. AUTH 기한과 확인 대기가 걸려 있던 연결도 있다. */
    private static Map<String, Endpoint> endedServers() {
        Map<String, Endpoint> ended = new LinkedHashMap<>();
        EndpointConfig timed = EndpointConfig.defaults().ackTimeoutMs(1000).authTimeoutMs(1000);
        Endpoint awaitingAuth = serverAwaitingAuth(timed);
        awaitingAuth.disconnect();
        ended.put("서버 AUTH 대기 중 끊김", awaitingAuth);
        Endpoint authExpiryPending = serverAwaitingAuth(timed);
        authExpiryPending.close(ErrorCode.PROTOCOL_ERROR);
        ended.put("서버 AUTH 대기 중 오류 CLOSE로 닫음", authExpiryPending);
        EndpointConfig idle = EndpointConfig.defaults().ackTimeoutMs(1000).idleIntervalMs(500);
        Endpoint pingOutstanding = openServer(idle);
        pingOutstanding.advance(600);
        pingOutstanding.disconnect();
        ended.put("서버 PING 확인 대기 중 끊김", pingOutstanding);
        Endpoint pingOutstandingClosed = openServer(idle);
        pingOutstandingClosed.advance(600);
        pingOutstandingClosed.close(ErrorCode.NO_ERROR);
        ended.put("서버 PING 확인 대기 중 NO_ERROR CLOSE로 닫음", pingOutstandingClosed);
        Endpoint pendingResponse = openServer();
        pendingResponse.receive(Frames.request(257, 1, new byte[0]));
        pendingResponse.disconnect();
        ended.put("서버 응답 대기 중 끊김", pendingResponse);
        return ended;
    }

    private static Map<String, Endpoint> endedEndpoints() {
        Map<String, Endpoint> ended = new LinkedHashMap<>(endedClients());
        ended.putAll(endedServers());
        ended.values().forEach(EndpointEndedConnectionTest::settle);
        return ended;
    }

    private static void forEachEnded(Consumer<Endpoint> check) {
        endedEndpoints().forEach((name, endpoint) -> {
            try {
                check.accept(endpoint);
            } catch (AssertionError e) {
                throw new AssertionError(name + ": " + e.getMessage(), e);
            }
        });
    }

    private static void assertSilent(Endpoint endpoint) {
        assertThat(endpoint.drainMessages()).isEmpty();
        assertThat(endpoint.drainEvents()).isEmpty();
    }

    @Test
    void receivedBytesAreIgnored() {
        forEachEnded(endpoint -> {
            endpoint.receive(bytes(PREFACE));
            endpoint.receive(bytes("0000000f0004"));
            endpoint.receive(bytes("ffffffff"));
            endpoint.receive(bytes("0000001c00030000000000000000000000000000" + "00000001" + "0000000000000000"));
            assertSilent(endpoint);
        });
    }

    @Test
    void receivedWebSocketMessagesAreIgnored() {
        Endpoint ended = settle(openWebSocketClient());
        ended.disconnect();
        settle(ended);

        ended.receiveMessage(WebSocketMessageKind.TEXT, bytes(HELLO));
        ended.receiveMessage(WebSocketMessageKind.BINARY, new byte[] {1});
        ended.receiveMessage(WebSocketMessageKind.BINARY, bytes(PREFACE));

        assertSilent(ended);
    }

    @Test
    void clockAdvanceIsIgnored() {
        forEachEnded(endpoint -> {
            endpoint.advance(1_000_000);
            endpoint.advance(1_000_000);
            assertSilent(endpoint);
        });
    }

    @Test
    void requestFailsLocallyWithAnUnavailableResultAndUsesATicket() {
        forEachEnded(endpoint -> {
            long first = endpoint.request(257, new byte[] {1});
            long second = endpoint.request(257, new byte[0], 100);

            assertThat(second).isEqualTo(first + 1);
            assertThat(endpoint.drainMessages()).isEmpty();
            List<Event> events = endpoint.drainEvents();
            assertThat(events).hasSize(2);
            for (Event event : events) {
                assertThat(event.kind()).isEqualTo(Event.Kind.RESULT);
                assertThat(event.streamId()).isZero();
                assertThat(event.status()).isEqualTo(Status.UNAVAILABLE);
                assertThat(event.certainty()).contains(Certainty.NOT_PROCESSED);
            }
            assertThat(endpoint.requestState(first)).isNull();
            assertThat(endpoint.openStreamCount()).isZero();
        });
    }

    @Test
    void requestTooLargeForThePeerStillFailsLocallyOnAnEndedConnection() {
        Endpoint ended = settle(openClient());
        ended.disconnect();
        settle(ended);

        ended.request(257, new byte[20_000]);

        assertThat(ended.drainMessages()).isEmpty();
        assertThat(ended.drainEvents()).hasSize(1);
    }

    @Test
    void cancellationsAreIgnored() {
        forEachEnded(endpoint -> {
            endpoint.cancelRequest(0);
            endpoint.cancelRequest(5);
            endpoint.cancel(1);
            endpoint.cancel(99);
            assertSilent(endpoint);
        });
    }

    @Test
    void requestsPendingBeforeTheEndAreNotCancelledAgain() {
        Endpoint client = openClient();
        long numbered = client.request(257, new byte[0]);
        client.drainMessages();
        client.drainEvents();
        client.disconnect();
        client.drainEvents();

        client.cancelRequest(numbered);
        client.cancel(1);

        assertSilent(client);
    }

    @Test
    void respondingToAPeerRequestIsIgnored() {
        Endpoint server = openServer();
        server.receive(Frames.request(257, 1, new byte[0]));
        settle(server);
        server.disconnect();
        settle(server);

        server.respond(1, 0, new byte[] {1});
        server.respond(3, 0, new byte[0]);

        assertSilent(server);
    }

    @Test
    void pingIsIgnored() {
        forEachEnded(endpoint -> {
            endpoint.ping(PING_PAYLOAD);
            endpoint.advance(1_000_000);
            assertSilent(endpoint);
        });
    }

    @Test
    void closeIsIgnored() {
        forEachEnded(endpoint -> {
            endpoint.close(ErrorCode.NO_ERROR);
            endpoint.close(ErrorCode.PROTOCOL_ERROR);
            assertSilent(endpoint);
        });
    }

    @Test
    void disconnectIsIgnored() {
        forEachEnded(endpoint -> {
            endpoint.disconnect();
            assertSilent(endpoint);
        });
    }

    @Test
    void abortClosingIsIgnored() {
        forEachEnded(endpoint -> {
            endpoint.abortClosing();
            assertSilent(endpoint);
        });
    }

    @Test
    void anEndedConnectionStaysEndedAfterEveryCallTogether() {
        List<Endpoint> ended = new ArrayList<>(endedEndpoints().values());
        for (Endpoint endpoint : ended) {
            endpoint.request(257, new byte[0]);
            endpoint.drainEvents();
            endpoint.close(ErrorCode.NO_ERROR);
            endpoint.abortClosing();
            endpoint.disconnect();
            assertSilent(endpoint);
        }
    }

    @Test
    void endingClearsTheLedger() {
        Endpoint client = settle(openClient());
        long written = client.request(257, new byte[0]);
        client.drainMessages();
        long numbered = client.request(257, new byte[0]);
        assertThat(client.openStreamCount()).isEqualTo(2);
        assertThat(client.requestState(written)).isNotNull();
        assertThat(client.requestState(numbered)).isNotNull();
        client.disconnect();
        settle(client);

        assertThat(client.openStreamCount()).isZero();
        assertThat(client.requestState(written)).isNull();
        assertThat(client.requestState(numbered)).isNull();
        client.cancel(1);
        client.cancel(3);
        client.cancelRequest(written);
        client.cancelRequest(numbered);
        assertSilent(client);
    }

    @Test
    void endingClearsThePeerPendingStreamsWithoutRefusingThem() {
        Endpoint server = settle(openServer());
        server.receive(Frames.request(257, 1, new byte[0]));
        server.receive(Frames.request(257, 3, new byte[0]));
        server.drainEvents();
        server.receive(Frames.request(257, 5, new byte[0]));
        server.disconnect();
        assertThat(server.drainMessages()).isEmpty();
        server.drainEvents();

        server.respond(1, 0, new byte[] {1});
        server.respond(3, 0, new byte[0]);
        server.respond(5, 0, new byte[0]);
        assertSilent(server);
    }
}
