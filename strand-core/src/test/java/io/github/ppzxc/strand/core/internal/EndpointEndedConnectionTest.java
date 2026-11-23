package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
        Drive.receive(client, bytes(HELLO));
        Drive.receive(client, bytes(HELLO_ACK));
        Drive.receive(client, bytes(AUTH_ACK));
        return client;
    }

    private static Endpoint serverAwaitingAuth(EndpointConfig config) {
        Endpoint server = Drive.server(config);
        Drive.receive(server, bytes(PREFACE));
        Drive.receive(server, bytes(HELLO));
        Drive.receive(server, bytes(HELLO_ACK));
        return server;
    }

    private static Endpoint openServer(EndpointConfig config) {
        Endpoint server = serverAwaitingAuth(config);
        Drive.receive(server, bytes(AUTH_ANONYMOUS));
        return server;
    }

    private static Endpoint openServer() {
        return openServer(EndpointConfig.defaults());
    }

    private static void receiveUnit(Endpoint endpoint, byte[] unit) {
        endpoint.receiveUnit(unit, 0, unit.length, CollectionSpace.UNLIMITED);
    }

    private static Endpoint openUnitInputClient() {
        Endpoint client = Endpoint.client(EndpointConfig.defaults());
        receiveUnit(client, bytes(HELLO));
        receiveUnit(client, bytes(HELLO_ACK));
        receiveUnit(client, bytes(AUTH_ACK));
        return client;
    }

    private static Endpoint settle(Endpoint endpoint) {
        Drive.drainMessages(endpoint);
        Drive.drainEvents(endpoint);
        return endpoint;
    }

    /** 끝난 클라이언트 연결들. 끝난 길과 끝난 시점이 서로 다르다. */
    private static Map<String, Endpoint> endedClients() {
        Map<String, Endpoint> ended = new LinkedHashMap<>();
        Endpoint beforeHello = Endpoint.client(EndpointConfig.defaults());
        beforeHello.disconnect();
        ended.put("연결 직후 끊김", beforeHello);
        Endpoint beforeAuth = Endpoint.client(EndpointConfig.defaults());
        Drive.receive(beforeAuth, bytes(HELLO));
        Drive.receive(beforeAuth, bytes(HELLO_ACK));
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
        Drive.receive(badFrame, bytes("0000000f0004" + "00000000000000000000000000"));
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
        Drive.advance(pingOutstanding, 600);
        pingOutstanding.disconnect();
        ended.put("서버 PING 확인 대기 중 끊김", pingOutstanding);
        Endpoint pingOutstandingClosed = openServer(idle);
        Drive.advance(pingOutstandingClosed, 600);
        pingOutstandingClosed.close(ErrorCode.NO_ERROR);
        ended.put("서버 PING 확인 대기 중 NO_ERROR CLOSE로 닫음", pingOutstandingClosed);
        Endpoint pendingResponse = openServer();
        Drive.receive(pendingResponse, Frames.request(257, 1, new byte[0]));
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
        assertThat(Drive.drainMessages(endpoint)).isEmpty();
        assertThat(Drive.drainEvents(endpoint)).isEmpty();
    }

    @Test
    void receivedBytesAreIgnored() {
        forEachEnded(endpoint -> {
            Drive.receive(endpoint, bytes(PREFACE));
            Drive.receive(endpoint, bytes("0000000f0004"));
            Drive.receive(endpoint, bytes("ffffffff"));
            Drive.receive(
                    endpoint, bytes("0000001c00030000000000000000000000000000" + "00000001" + "0000000000000000"));
            assertSilent(endpoint);
        });
    }

    @Test
    void receivedUnitInputsAreIgnored() {
        Endpoint ended = settle(openUnitInputClient());
        ended.disconnect();
        settle(ended);

        ended.receiveUnsupportedInput(CollectionSpace.UNLIMITED);
        receiveUnit(ended, new byte[] {1});
        receiveUnit(ended, bytes(PREFACE));

        assertSilent(ended);
    }

    @Test
    void clockAdvanceIsIgnored() {
        forEachEnded(endpoint -> {
            Drive.advance(endpoint, 1_000_000);
            Drive.advance(endpoint, 1_000_000);
            assertSilent(endpoint);
        });
    }

    @Test
    void requestFailsLocallyWithAnUnavailableResultAndUsesATicket() {
        forEachEnded(endpoint -> {
            long first = endpoint.request(257, new byte[] {1});
            long second = endpoint.request(257, new byte[0], 100);

            assertThat(second).isEqualTo(first + 1);
            assertThat(Drive.drainMessages(endpoint)).isEmpty();
            List<Event> events = Drive.drainEvents(endpoint);
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

        assertThat(Drive.drainMessages(ended)).isEmpty();
        assertThat(Drive.drainEvents(ended)).hasSize(1);
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
        Drive.drainMessages(client);
        Drive.drainEvents(client);
        client.disconnect();
        Drive.drainEvents(client);

        client.cancelRequest(numbered);
        client.cancel(1);

        assertSilent(client);
    }

    @Test
    void respondingToAPeerRequestIsIgnored() {
        Endpoint server = openServer();
        Drive.receive(server, Frames.request(257, 1, new byte[0]));
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
            Drive.advance(endpoint, 1_000_000);
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
            Drive.drainEvents(endpoint);
            endpoint.close(ErrorCode.NO_ERROR);
            endpoint.abortClosing();
            endpoint.disconnect();
            assertSilent(endpoint);
        }
    }

    /** 인증 콜백 안에서 {@code onCallback}이 연결을 끝내도록 한 서버로 AUTH를 받게 하고, 그 서버를 돌려준다. */
    private static Endpoint serverEndedInsideAuthCallback(AuthAction onCallback) {
        AtomicReference<Endpoint> self = new AtomicReference<>();
        Endpoint server = Drive.server(EndpointConfig.defaults(), (mechanism, credentials) -> {
            self.get().disconnect();
            return onCallback.decide();
        });
        self.set(server);
        Drive.receive(server, bytes(PREFACE));
        Drive.receive(server, bytes(HELLO));
        Drive.receive(server, bytes(HELLO_ACK));
        Drive.drainMessages(server);
        Drive.drainEvents(server);
        Drive.receive(server, bytes(AUTH_ANONYMOUS));
        return server;
    }

    @FunctionalInterface
    private interface AuthAction {
        AuthDecision decide();
    }

    @Test
    void authDecisionAfterTheConnectionEndedInsideTheCallbackQueuesNoFrames() {
        for (AuthDecision decision : AuthDecision.values()) {
            Endpoint server = serverEndedInsideAuthCallback(() -> decision);

            assertThat(Drive.drainMessages(server)).as(decision.name()).isEmpty();
            assertThat(Drive.drainEvents(server))
                    .as(decision.name())
                    .extracting(Event::kind)
                    .containsExactly(Event.Kind.CLOSED);
        }
    }

    @Test
    void authenticatorThrowingAfterTheConnectionEndedQueuesNoFramesAndNoSecondClosed() {
        Endpoint server = serverEndedInsideAuthCallback(() -> {
            throw new IllegalStateException("late");
        });

        assertThat(Drive.drainMessages(server)).isEmpty();
        List<Event> events = Drive.drainEvents(server);
        assertThat(events).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
        assertThat(events.get(0).cause()).isEmpty();
    }

    @Test
    void closingInsideTheAuthCallbackEndsTheConnectionSoNoAuthAckFollowsTheClose() {
        // AUTH 단계에는 대기 중인 스트림이 없어, 콜백 안의 CLOSE는 곧바로 연결을 끝낸다. "닫는 중이지만 안 끝난" 상태는 생기지 않는다.
        for (int code : new int[] {ErrorCode.NO_ERROR, ErrorCode.PROTOCOL_ERROR}) {
            AtomicReference<Endpoint> self = new AtomicReference<>();
            Endpoint server = Drive.server(EndpointConfig.defaults(), (mechanism, credentials) -> {
                self.get().close(code);
                return AuthDecision.PASS;
            });
            self.set(server);
            Drive.receive(server, bytes(PREFACE));
            Drive.receive(server, bytes(HELLO));
            Drive.receive(server, bytes(HELLO_ACK));
            Drive.drainMessages(server);
            Drive.drainEvents(server);

            Drive.receive(server, bytes(AUTH_ANONYMOUS));

            assertThat(Drive.drainMessages(server)).as("code " + code).hasSize(1); // CLOSE 하나뿐, AUTH ACK 없음
            assertThat(Drive.drainEvents(server)).extracting(Event::kind).containsExactly(Event.Kind.CLOSED);
        }
    }

    @Test
    void endingClearsAWrittenRequestFromTheLedger() {
        Endpoint client = settle(openClient());
        long written = client.request(257, new byte[0]);
        Drive.drainMessages(client);
        assertThat(client.openStreamCount()).isEqualTo(1);
        assertThat(client.requestState(written)).isNotNull();
        client.disconnect();
        settle(client);

        assertThat(client.openStreamCount()).isZero();
        assertThat(client.requestState(written)).isNull();
        // 클라이언트가 처음 연 스트림은 1번이다(규격 2.2.5절). 장부가 비워졌으므로 취소해도 아무것도 나가지 않는다.
        client.cancel(1);
        client.cancelRequest(written);
        assertSilent(client);
    }

    @Test
    void endingClearsAnUnwrittenRequestFromTheLedger() {
        Endpoint client = settle(openClient());
        long numbered = client.request(257, new byte[0]);
        assertThat(client.openStreamCount()).isEqualTo(1);
        assertThat(client.requestState(numbered)).isNotNull();
        client.disconnect();
        settle(client);

        assertThat(client.openStreamCount()).isZero();
        assertThat(client.requestState(numbered)).isNull();
        client.cancelRequest(numbered);
        assertSilent(client);
    }

    @Test
    void endingClearsThePeerPendingStreamsWithoutRefusingThem() {
        Endpoint server = settle(openServer());
        Drive.receive(server, Frames.request(257, 1, new byte[0]));
        Drive.receive(server, Frames.request(257, 3, new byte[0]));
        Drive.drainEvents(server);
        Drive.receive(server, Frames.request(257, 5, new byte[0]));
        // 끝나기 전에는 대기 스트림이 있어 응답이 나간다.
        server.respond(1, 0, new byte[] {1});
        assertThat(Drive.drainMessages(server)).hasSize(1);
        server.disconnect();
        assertThat(Drive.drainMessages(server)).isEmpty();
        // 끊김은 돌려보내지 않으므로 꺼내지 않은 요청 5의 이벤트가 그대로 남고, 뒤에 CLOSED가 온다.
        assertThat(Drive.drainEvents(server))
                .extracting(Event::kind)
                .containsExactly(Event.Kind.REQUEST_RECEIVED, Event.Kind.CLOSED);

        // 대기 스트림 표는 private이라 respond의 무반응으로 비워졌음을 본다. 표를 비우지 않으면 응답 프레임이 나가 실패한다.
        // 3은 꺼낸 요청, 5는 꺼내지 않은 요청이다.
        server.respond(3, 0, new byte[0]);
        server.respond(5, 0, new byte[0]);
        assertSilent(server);
    }
}
