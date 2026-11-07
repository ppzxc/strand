package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.NO_ERROR;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Event;
import java.util.ArrayList;
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
     * 마지막 스트림의 결과보다 먼저 나도 실패하므로 이벤트 순서도 함께 본다. 서버가 CLOSE를 받은 뒤 응답하는지는 API로 관찰할 수 없어서, 응답이 CLOSE보다
     * 먼저 닿는 경우도 같은 결과여야 한다.
     */
    @Test
    @DisplayName("대기 요청이 있는 채 NO_ERROR CLOSE를 보내도 응답을 받은 뒤에 닫힌다 (규격 3.5)")
    void noErrorCloseWaitsForPendingResponse() throws Exception {
        try (TestServer server = TestServer.start();
                TestClient client = TestClient.connect(server.port())) {
            Connection serverConnection = server.awaitConnection();
            client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
            Event received = serverConnection.awaitEvent(Event.Kind.REQUEST_RECEIVED);

            client.closeConnection(NO_ERROR);
            serverConnection.withEndpoint(e -> e.respond(received.streamId(), STATUS_OK, Fixtures.payload("late")));
            client.awaitFinished();
            serverConnection.awaitFinished();

            List<Event> clientEvents = client.events();
            assertThat(kinds(clientEvents)).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
            assertThat(clientEvents.get(0).status()).isEqualTo(STATUS_OK);
            assertThat(kinds(serverConnection.events()))
                    .containsExactly(Event.Kind.REQUEST_RECEIVED, Event.Kind.CLOSED);
        }
    }

    /**
     * 양쪽이 동시에 {@code NO_ERROR} CLOSE를 보내면 실제 TCP에서 두 CLOSE가 엇갈릴 수 있다. 엇갈려도 양쪽 {@code closed}는 정확히 한 번씩이어야 하고
     * 두 번째 CLOSE를 보내서는 안 된다(규격 3.5절 "양쪽이 동시에 CLOSE를 보내 엇갈리는 것은 허용한다"). 중복되면 이벤트 목록에 {@code CLOSED}가 둘이
     * 되거나 읽기 스레드가 예외로 끝난다. 엇갈림은 우연이라 시작 신호로 양쪽을 동시에 출발시켜 {@value #REPEATS}번 되풀이한다.
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

                    assertThat(kinds(client.events()))
                            .as("%d번째 클라이언트 이벤트", round)
                            .containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
                    assertThat(kinds(serverConnection.events()))
                            .as("%d번째 서버 쪽 이벤트", round)
                            .containsExactly(Event.Kind.REQUEST_RECEIVED, Event.Kind.CLOSED);
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private static List<Event.Kind> kinds(List<Event> events) {
        List<Event.Kind> kinds = new ArrayList<>();
        for (Event event : events) {
            kinds.add(event.kind());
        }
        return kinds;
    }
}
