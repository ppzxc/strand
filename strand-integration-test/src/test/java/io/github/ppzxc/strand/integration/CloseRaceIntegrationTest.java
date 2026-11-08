package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.CLOSE_FRAME_BYTES;
import static io.github.ppzxc.strand.integration.Fixtures.NO_ERROR;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.Event;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 종료가 다른 동작과 겹치는 경우를 실제 TCP 위에서 확인한다(규격 3.5절). */
class CloseRaceIntegrationTest {

    /** CLOSE가 엇갈리는 것은 타이밍에 달려 있어서, 같은 시나리오를 여러 번 되풀이해 엇갈릴 기회를 늘린다. */
    private static final int REPEATS = 20;

    /**
     * {@code NO_ERROR} CLOSE는 대기 중인 스트림이 끝날 때까지 연결을 닫지 않는다. 서버가 응답을 보류한 채 클라이언트가 CLOSE를 보내고, 그 뒤에 서버가
     * 응답한다. 대기 중인 요청을 버리고 바로 닫는 구현은 클라이언트가 {@code OK}가 아니라 로컬 {@code UNAVAILABLE}을 받아 실패한다. {@code closed}가
     * 마지막 스트림의 결과보다 먼저 나도 실패하므로 이벤트 순서도 함께 본다. 서버가 CLOSE를 처리했는지는 이벤트로 볼 수 없어서, 서버가 처리한 바이트 수가 CLOSE
     * 프레임 크기만큼 늘어난 것을 확인한 뒤에 응답한다. 응답이 CLOSE보다 먼저 닿는 경로로 통과하지 않게 하려는 것이다.
     */
    @Test
    @DisplayName("대기 요청이 있는 채 NO_ERROR CLOSE를 보내도 응답을 받은 뒤에 닫힌다 (규격 3.5)")
    void noErrorCloseWaitsForPendingResponse() throws Exception {
        try (TestServer server = TestServer.start();
                TestClient client = TestClient.connect(server.port())) {
            Connection serverConnection = server.awaitConnection();
            client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
            Event received = serverConnection.awaitEvent(Event.Kind.REQUEST_RECEIVED);
            long receivedBeforeClose = serverConnection.receivedBytes();

            client.closeConnection(NO_ERROR);
            serverConnection.awaitReceivedBytes(receivedBeforeClose + CLOSE_FRAME_BYTES);
            serverConnection.withEndpoint(e -> e.respond(received.streamId(), STATUS_OK, Fixtures.payload("late")));
            client.awaitFinished();
            serverConnection.awaitFinished();

            List<Event> clientEvents = client.events();
            assertThat(Fixtures.kinds(clientEvents)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
            assertThat(clientEvents.get(0).status()).isEqualTo(STATUS_OK);
            assertThat(clientEvents.get(0).certainty()).contains(Certainty.RESPONSE);
            assertThat(Fixtures.kinds(serverConnection.events()))
                    .containsExactly(Event.Kind.REQUEST_RECEIVED, Event.Kind.CLOSED);
        }
    }

    /**
     * 양쪽에 대기 중인 스트림이 있으면 {@code NO_ERROR} CLOSE를 보내도 연결은 닫히지 않고 종료 중이 된다. 이 상태에서 상대의 CLOSE가 오면(엇갈림) 이미 CLOSE를
     * 보냈으므로 답하는 CLOSE를 다시 보내서는 안 된다(규격 3.5절 "CLOSE를 보냈거나 받은 뒤에는 CLOSE를 보내지 않는다(MUST NOT)", "양쪽이 동시에 CLOSE를 보내
     * 엇갈리는 것은 허용한다"). 서로 요청을 낸 뒤 응답을 보류하고, 두 연결의 잠금을 함께 잡아 양쪽이 서로의 CLOSE를 보기 전에 각자 CLOSE를 보내게 한다. 둘 다 상대
     * CLOSE를 처리한 뒤에 각자 쓴 바이트가 CLOSE 프레임 하나(32바이트)인지 보고, 그다음 서로 응답해 끝낸다. 두 번째 CLOSE를 보내면 그쪽이 쓴 바이트가 64가 되고,
     * {@code closed}가 중복되면 이벤트 목록에 {@code CLOSED}가 둘이 된다. 대기 스트림이 없으면 CLOSE를 보내는 즉시 닫혀 상대 CLOSE를 처리하지 않으므로 이 경로를
     * 시험할 수 없다.
     */
    @Test
    @DisplayName("대기 스트림이 있는 양쪽이 서로의 CLOSE를 보기 전에 CLOSE를 보내도 CLOSE는 각자 하나뿐이다 (규격 3.5)")
    void crossedCloseWhileClosingSendsOneCloseEach() throws Exception {
        try (TestServer server = TestServer.start();
                TestClient client = TestClient.connect(server.port())) {
            Connection serverConnection = server.awaitConnection();
            client.request(APPLICATION_TYPE, Fixtures.payload("from-client"));
            serverConnection.withEndpoint(e -> e.request(APPLICATION_TYPE, Fixtures.payload("from-server")));
            Event requestAtServer = serverConnection.awaitEvent(Event.Kind.REQUEST_RECEIVED);
            Event requestAtClient = client.awaitEvent(Event.Kind.REQUEST_RECEIVED);
            long clientWrittenBefore = client.writtenBytes();
            long serverWrittenBefore = serverConnection.writtenBytes();
            long clientReceivedBefore = client.connection().receivedBytes();
            long serverReceivedBefore = serverConnection.receivedBytes();

            Connection.withBoth(client.connection(), e -> e.close(NO_ERROR), serverConnection, e -> e.close(NO_ERROR));
            client.connection().awaitReceivedBytes(clientReceivedBefore + CLOSE_FRAME_BYTES);
            serverConnection.awaitReceivedBytes(serverReceivedBefore + CLOSE_FRAME_BYTES);

            assertThat(client.writtenBytes() - clientWrittenBefore).isEqualTo(CLOSE_FRAME_BYTES);
            assertThat(serverConnection.writtenBytes() - serverWrittenBefore).isEqualTo(CLOSE_FRAME_BYTES);

            serverConnection.withEndpoint(
                    e -> e.respond(requestAtServer.streamId(), STATUS_OK, Fixtures.payload("ok")));
            client.connection()
                    .withEndpoint(e -> e.respond(requestAtClient.streamId(), STATUS_OK, Fixtures.payload("ok")));
            client.awaitFinished();
            serverConnection.awaitFinished();

            assertThat(Fixtures.kinds(client.events()))
                    .containsExactly(Event.Kind.REQUEST_RECEIVED, Event.Kind.RESULT, Event.Kind.CLOSED);
            assertThat(Fixtures.kinds(serverConnection.events()))
                    .containsExactly(Event.Kind.REQUEST_RECEIVED, Event.Kind.RESULT, Event.Kind.CLOSED);
        }
    }

    /**
     * 대기 스트림이 없는 양쪽이 동시에 {@code NO_ERROR} CLOSE를 보내면 각자 CLOSE를 보내는 즉시 닫히고, 뒤따르는 상대 CLOSE와 TCP 끊김은 무시되어야 한다. 그래도 {@code
     * closed}는 양쪽에서 정확히 한 번씩이어야 한다(규격 3.5절 "연결당 정확히 한 번"). 중복되면 이벤트 목록에 {@code CLOSED}가 둘이 된다. 실제 타이밍에서 양쪽이
     * 엇갈리는지는 보장되지 않으므로 시작 신호로 동시에 출발시켜 {@value #REPEATS}번 되풀이한다. 엇갈림을 확정해서 보는 시험은 {@link
     * #crossedCloseWhileClosingSendsOneCloseEach()}가 맡는다.
     */
    @Test
    @DisplayName("양쪽이 동시에 CLOSE를 보내도 closed는 양쪽에서 한 번씩만 난다 (규격 3.5)")
    void simultaneousCloseEmitsClosedOncePerSide() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (TestServer server = TestServer.startResponding(type -> STATUS_OK)) {
            for (int round = 0; round < REPEATS; round++) {
                try (TestClient client = TestClient.connect(server.port())) {
                    Connection serverConnection = server.awaitConnection();
                    client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
                    client.awaitEvent(Event.Kind.RESULT);

                    CountDownLatch start = new CountDownLatch(1);
                    Future<?> clientClose = pool.submit(() -> {
                        start.await();
                        client.closeConnection(NO_ERROR);
                        return null;
                    });
                    Future<?> serverClose = pool.submit(() -> {
                        start.await();
                        serverConnection.withEndpoint(e -> e.close(NO_ERROR));
                        return null;
                    });
                    start.countDown();
                    clientClose.get(Connection.TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    serverClose.get(Connection.TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    client.awaitFinished();
                    serverConnection.awaitFinished();

                    assertThat(Fixtures.kinds(client.events()))
                            .as("%d번째 클라이언트 이벤트", round)
                            .containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
                    assertThat(Fixtures.kinds(serverConnection.events()))
                            .as("%d번째 서버 쪽 이벤트", round)
                            .containsExactly(Event.Kind.REQUEST_RECEIVED, Event.Kind.CLOSED);
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
