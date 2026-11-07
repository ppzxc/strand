package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
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

/** 서버 하나에 클라이언트 여러 개가 붙는 1:N 연결을 실제 TCP 위에서 확인한다. 연결마다 {@code Endpoint}가 독립이라는 점이 대상이다. */
class MultiClientIntegrationTest {

    private static final int CLIENT_COUNT = 16;
    /** 규격 4.2절: 클라이언트가 처음 여는 스트림은 1번이다. 연결마다 번호 공간이 따로라 모든 클라이언트가 1번을 쓴다. */
    private static final int FIRST_CLIENT_STREAM_ID = 1;

    /** 서버는 요청 {@code type}에서 {@code APPLICATION_TYPE}를 뺀 값을 {@code status}로 응답한다. 클라이언트 {@code i}는 {@code status} {@code i}를 기대한다. */
    private static TestServer startServer() throws Exception {
        return TestServer.startResponding(type -> type - APPLICATION_TYPE);
    }

    /**
     * 연결마다 {@code Endpoint}가 따로여야 하는데 상태를 공유하면 응답이 다른 연결로 새거나 스트림 번호가 섞인다. 클라이언트 16개가 동시에 연결해 각자 다른
     * {@code type}으로 요청을 내고, 각 클라이언트가 자기 요청에 해당하는 {@code status}를 자기 스트림 1번으로 받는지 본다. 모든 클라이언트가 같은
     * {@code stream_id} 1을 쓰므로, 연결 사이에 번호 공간이 섞이면 실패한다.
     */
    @Test
    @DisplayName("클라이언트 16개가 동시에 연결해도 응답이 자기 연결로만 돌아온다 (규격 4.2)")
    void concurrentClientsEachGetTheirOwnResponse() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(CLIENT_COUNT);
        try (TestServer server = startServer()) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Event>> results = new ArrayList<>();
            for (int i = 0; i < CLIENT_COUNT; i++) {
                int index = i;
                results.add(pool.submit(() -> connectAndRequest(server.port(), index, start)));
            }
            start.countDown();

            for (int i = 0; i < CLIENT_COUNT; i++) {
                Event result = results.get(i).get(Connection.TIMEOUT_MS, TimeUnit.MILLISECONDS);
                assertThat(result.streamId()).as("클라이언트 %d의 stream_id", i).isEqualTo(FIRST_CLIENT_STREAM_ID);
                assertThat(result.status()).as("클라이언트 %d의 status", i).isEqualTo(i);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /** 신호가 올 때까지 기다렸다가 연결하고 요청을 내서, 연결과 요청이 한꺼번에 몰리게 한다. */
    private static Event connectAndRequest(int port, int index, CountDownLatch start) throws Exception {
        start.await();
        try (TestClient client = TestClient.connect(port)) {
            client.request(APPLICATION_TYPE + index, Fixtures.payload("client-" + index));
            return client.awaitEvent(Event.Kind.RESULT);
        }
    }

    /**
     * 한 연결이 CLOSE 없이 끊겨도 그 연결에서 끝나야 한다. 실패가 다른 연결의 이벤트나 응답에 번지면 나머지 클라이언트가 {@code closed}를 받거나 응답을
     * 받지 못한다. 클라이언트 16개 중 하나만 TCP를 끊은 뒤, 서버가 그 연결에서만 {@code closed}를 한 번 내고 나머지 15개는 {@code closed} 없이 요청과
     * 응답을 계속 주고받는지 본다(규격 3.5절, 4.2절).
     */
    @Test
    @DisplayName("한 연결이 비정상 종료돼도 나머지 연결은 영향을 받지 않는다 (규격 3.5, 4.2)")
    void oneAbruptDisconnectDoesNotAffectOthers() throws Exception {
        List<TestClient> clients = new ArrayList<>();
        List<Connection> serverSides = new ArrayList<>();
        try (TestServer server = startServer()) {
            for (int i = 0; i < CLIENT_COUNT; i++) {
                clients.add(TestClient.connect(server.port()));
                // 연결을 하나씩 맺고 바로 받으므로, 받은 서버 쪽 연결은 방금 연결한 클라이언트의 짝이다.
                serverSides.add(server.awaitConnection());
            }

            int aborted = 0;
            clients.get(aborted).close();
            serverSides.get(aborted).awaitFinished();

            for (int i = 0; i < CLIENT_COUNT; i++) {
                if (i == aborted) {
                    continue;
                }
                TestClient client = clients.get(i);
                client.request(APPLICATION_TYPE + i, Fixtures.payload("client-" + i));
                assertThat(client.awaitEvent(Event.Kind.RESULT).status()).isEqualTo(i);
            }
            assertThat(closedCount(serverSides.get(aborted).events())).isEqualTo(1);
            for (int i = 0; i < CLIENT_COUNT; i++) {
                if (i != aborted) {
                    assertThat(closedCount(serverSides.get(i).events()))
                            .as("서버 쪽 연결 %d의 closed", i)
                            .isZero();
                }
            }
        } finally {
            for (TestClient client : clients) {
                client.close();
            }
        }
    }

    private static long closedCount(List<Event> events) {
        return events.stream().filter(e -> e.kind() == Event.Kind.CLOSED).count();
    }
}
