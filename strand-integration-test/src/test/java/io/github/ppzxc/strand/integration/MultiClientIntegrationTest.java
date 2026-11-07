package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.NO_ERROR;
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

/**
 * 서버 하나에 클라이언트 여러 개가 붙는 1:N 연결을 실제 TCP 위에서 확인한다. 연결마다 {@code Endpoint}가 독립이라는 점이 대상이다.
 *
 * <p>서버는 요청 {@code type}에서 {@code APPLICATION_TYPE}를 뺀 값을 {@code status}로 응답한다. 클라이언트 {@code i}는 {@code type}
 * {@code APPLICATION_TYPE + i}로 요청하고 {@code status} {@code i}를 기대한다. {@code Event}에는 페이로드 접근자가 없어서, 응답이 자기 연결로
 * 왔는지는 이 {@code status}로 가른다.
 */
class MultiClientIntegrationTest {

    private static final int CLIENT_COUNT = 16;
    /** 규격 4.2절: 클라이언트가 처음 여는 스트림은 1번이다. 연결마다 번호 공간이 따로라 모든 클라이언트가 1번을 쓴다. */
    private static final int FIRST_CLIENT_STREAM_ID = 1;

    private static TestServer startServer() throws Exception {
        return TestServer.startResponding(type -> type - APPLICATION_TYPE);
    }

    private static void requestFor(TestClient client, int index) {
        client.request(APPLICATION_TYPE + index, Fixtures.payload("client-" + index));
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
            requestFor(client, index);
            return client.awaitEvent(Event.Kind.RESULT);
        }
    }

    /**
     * 수립된 연결 하나가 CLOSE 없이 끊겨도 그 연결에서만 끝나야 한다. 실패가 다른 연결에 번지면 나머지 클라이언트가 {@code closed}를 일찍 받거나 응답을 받지
     * 못한다. 클라이언트 16개가 모두 요청·응답을 한 번 주고받아 연결을 수립한 뒤, 15개가 두 번째 요청을 낸 채로 하나를 끊는다. 마지막에 나머지를 정상 종료하고
     * 이벤트 목록 전체를 순서까지 비교한다. 끊긴 쪽은 요청 한 건 뒤 {@code closed}로 끝나고, 나머지는 {@code closed}가 자기 정상 종료 뒤에 한 번만 나야 한다. 부정
     * 단언 없이 목록 전체를 비교하므로 {@code closed}가 늦게 번져도 놓치지 않는다(규격 3.5절, 4.2절).
     */
    @Test
    @DisplayName("수립된 연결 하나가 비정상 종료돼도 나머지 연결은 영향을 받지 않는다 (규격 3.5, 4.2)")
    void oneAbruptDisconnectDoesNotAffectOthers() throws Exception {
        int aborted = 0;
        List<TestClient> clients = new ArrayList<>();
        List<Connection> serverSides = new ArrayList<>();
        try (TestServer server = startServer()) {
            for (int i = 0; i < CLIENT_COUNT; i++) {
                clients.add(TestClient.connect(server.port()));
                // 연결을 하나씩 맺고 바로 받으므로, 받은 서버 쪽 연결은 방금 연결한 클라이언트의 짝이다.
                serverSides.add(server.awaitConnection());
            }
            for (int i = 0; i < CLIENT_COUNT; i++) {
                requestFor(clients.get(i), i);
            }
            for (int i = 0; i < CLIENT_COUNT; i++) {
                assertThat(clients.get(i).awaitEvent(Event.Kind.RESULT).status())
                        .isEqualTo(i);
            }

            for (int i = 0; i < CLIENT_COUNT; i++) {
                if (i != aborted) {
                    requestFor(clients.get(i), i);
                }
            }
            clients.get(aborted).close();
            serverSides.get(aborted).awaitFinished();
            for (int i = 0; i < CLIENT_COUNT; i++) {
                if (i != aborted) {
                    assertThat(clients.get(i).awaitEvent(Event.Kind.RESULT).status())
                            .isEqualTo(i);
                }
            }

            for (int i = 0; i < CLIENT_COUNT; i++) {
                if (i != aborted) {
                    clients.get(i).closeConnection(NO_ERROR);
                    clients.get(i).awaitFinished();
                }
                serverSides.get(i).awaitFinished();
            }

            assertThat(Fixtures.kinds(serverSides.get(aborted).events()))
                    .containsExactly(Event.Kind.REQUEST_RECEIVED, Event.Kind.CLOSED);
            for (int i = 0; i < CLIENT_COUNT; i++) {
                if (i != aborted) {
                    assertThat(Fixtures.kinds(clients.get(i).events()))
                            .as("클라이언트 %d의 이벤트", i)
                            .containsExactly(Event.Kind.RESULT, Event.Kind.RESULT, Event.Kind.CLOSED);
                    assertThat(Fixtures.kinds(serverSides.get(i).events()))
                            .as("서버 쪽 연결 %d의 이벤트", i)
                            .containsExactly(
                                    Event.Kind.REQUEST_RECEIVED, Event.Kind.REQUEST_RECEIVED, Event.Kind.CLOSED);
                }
            }
        } finally {
            for (TestClient client : clients) {
                client.close();
            }
        }
    }
}
