package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.nio.internal.ConnectionClosed;
import io.github.ppzxc.strand.nio.internal.IncomingRequest;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
 * {@code APPLICATION_TYPE + i}로 요청하고 {@code status} {@code i}를 기대한다. 응답이 자기 연결로 왔는지는 이 {@code status}로 가른다.
 */
class MultiClientIntegrationTest {

    private static final int CLIENT_COUNT = 16;
    /** 규격 4.2절: 클라이언트가 처음 여는 스트림은 1번이다. 연결마다 번호 공간이 따로라 모든 클라이언트가 1번을 쓴다. */
    private static final long FIRST_CLIENT_STREAM_ID = 1L;

    private static TcpServer startServer() throws Exception {
        return TcpServer.start(type -> type - APPLICATION_TYPE);
    }

    private static CompletableFuture<RequestResult> requestFor(Peer client, int index) {
        CompletableFuture<RequestResult> result = client.request(
                        APPLICATION_TYPE + index, Fixtures.payload("client-" + index))
                .result();
        client.logResult(result);
        return result;
    }

    private static RequestResult await(CompletableFuture<RequestResult> future) throws Exception {
        return future.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * 연결마다 {@code Endpoint}가 따로여야 하는데 상태를 공유하면 응답이 다른 연결로 새거나 스트림 번호가 섞인다. 클라이언트 16개가 동시에 연결해 각자 다른
     * {@code type}으로 요청을 내고, 각 클라이언트가 자기 요청에 해당하는 {@code status}를 받는지, 서버가 받은 요청이 모두 스트림 1번인지 본다. 모든
     * 클라이언트가 같은 {@code stream_id} 1을 쓰므로, 연결 사이에 번호 공간이 섞이면 실패한다.
     */
    @Test
    @DisplayName("클라이언트 16개가 동시에 연결해도 응답이 자기 연결로만 돌아온다 (규격 4.2)")
    void concurrentClientsEachGetTheirOwnResponse() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(CLIENT_COUNT);
        try (TcpServer server = startServer()) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<RequestResult>> results = new ArrayList<>();
            for (int i = 0; i < CLIENT_COUNT; i++) {
                int index = i;
                results.add(pool.submit(() -> connectAndRequest(server.port(), index, start)));
            }
            start.countDown();

            for (int i = 0; i < CLIENT_COUNT; i++) {
                RequestResult result = results.get(i).get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
                assertThat(result.certainty()).as("클라이언트 %d의 결과 확정성", i).isEqualTo(Certainty.RESPONSE);
                assertThat(result.status()).as("클라이언트 %d의 status", i).isEqualTo(i);
            }
            Map<Integer, Long> streamByType = new HashMap<>();
            for (int i = 0; i < CLIENT_COUNT; i++) {
                IncomingRequest request = server.awaitPeer().awaitRequest();
                streamByType.put(request.type(), request.streamId());
            }
            assertThat(streamByType)
                    .hasSize(CLIENT_COUNT)
                    .allSatisfy((type, streamId) -> assertThat(streamId).isEqualTo(FIRST_CLIENT_STREAM_ID));
        } finally {
            pool.shutdownNow();
        }
    }

    /** 신호가 올 때까지 기다렸다가 연결하고 요청을 내서, 연결과 요청이 한꺼번에 몰리게 한다. */
    private static RequestResult connectAndRequest(int port, int index, CountDownLatch start) throws Exception {
        start.await();
        try (Peer client = Peer.connect(port, null)) {
            return await(requestFor(client, index));
        }
    }

    /**
     * 수립된 연결 하나가 CLOSE 없이 끊겨도 그 연결에서만 끝나야 한다. 실패가 다른 연결에 번지면 나머지 클라이언트가 종료 알림을 일찍 받거나 응답을 받지
     * 못한다. 클라이언트 16개가 모두 요청·응답을 한 번 주고받아 연결을 수립한 뒤, 15개가 두 번째 요청을 낸 채로 하나를 끊는다. 마지막에 나머지에 연결 종료(CLOSE)를 보내고
     * 연결마다 받은 요청 수와 종료 알림을 비교한다. 끊긴 쪽은 요청 한 건을 받고 CLOSE 없이 끊김으로 한 번, 나머지는 자기 연결 종료(CLOSE)로 한 번만 알림을 받아야
     * 한다. 알림이 늦게 번져 한 번 더 오는 것도 연결을 모두 닫은 뒤의 알림 수로 놓치지 않는다(규격 3.5절, 4.2절).
     */
    @Test
    @DisplayName("수립된 연결 하나가 비정상 종료돼도 나머지 연결은 영향을 받지 않는다 (규격 3.5, 4.2)")
    void oneAbruptDisconnectDoesNotAffectOthers() throws Exception {
        int aborted = 0;
        List<Peer> clients = new ArrayList<>();
        List<Peer> serverSides = new ArrayList<>();
        try (TcpServer server = startServer()) {
            for (int i = 0; i < CLIENT_COUNT; i++) {
                clients.add(Peer.connect(server.port(), null));
                // 연결을 하나씩 맺고 바로 받으므로, 받은 서버 쪽 연결은 방금 연결한 클라이언트의 짝이다.
                serverSides.add(server.awaitPeer());
            }
            List<CompletableFuture<RequestResult>> first = new ArrayList<>();
            for (int i = 0; i < CLIENT_COUNT; i++) {
                first.add(requestFor(clients.get(i), i));
            }
            for (int i = 0; i < CLIENT_COUNT; i++) {
                RequestResult result = await(first.get(i));
                assertThat(result.status()).isEqualTo(i);
                assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
            }

            List<CompletableFuture<RequestResult>> second = new ArrayList<>();
            for (int i = 0; i < CLIENT_COUNT; i++) {
                if (i != aborted) {
                    second.add(requestFor(clients.get(i), i));
                }
            }
            clients.get(aborted).stop();
            ConnectionClosed abortedNotice = serverSides.get(aborted).awaitClosed();
            for (int i = 0; i < second.size(); i++) {
                int client = i < aborted ? i : i + 1;
                assertThat(await(second.get(i)).status()).isEqualTo(client);
            }

            for (int i = 0; i < CLIENT_COUNT; i++) {
                if (i != aborted) {
                    clients.get(i).driver().closeConnection();
                    clients.get(i).awaitClosed();
                }
                serverSides.get(i).awaitClosed();
            }
            for (Peer peer : clients) {
                peer.awaitResultsLogged();
                peer.stop();
            }
            for (Peer peer : serverSides) {
                peer.stop();
            }

            assertThat(abortedNotice.reason().kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
            assertThat(serverSides.get(aborted).closedNotices()).hasSize(1);
            assertThat(serverSides.get(aborted).drainRequests()).hasSize(1);
            assertThat(serverSides.get(aborted).log()).containsExactly(Peer.REQUEST_RECEIVED, Peer.CLOSED);
            for (int i = 0; i < CLIENT_COUNT; i++) {
                if (i != aborted) {
                    assertThat(clients.get(i).closedNotices())
                            .as("클라이언트 %d의 종료 알림", i)
                            .hasSize(1);
                    assertThat(clients.get(i).closedNotices().get(0).reason().kind())
                            .isEqualTo(CloseReason.Kind.CLOSE);
                    assertThat(serverSides.get(i).closedNotices())
                            .as("서버 쪽 연결 %d의 종료 알림", i)
                            .hasSize(1);
                    assertThat(serverSides
                                    .get(i)
                                    .closedNotices()
                                    .get(0)
                                    .reason()
                                    .kind())
                            .isEqualTo(CloseReason.Kind.CLOSE);
                    assertThat(serverSides.get(i).drainRequests())
                            .as("서버 쪽 연결 %d의 받은 요청", i)
                            .hasSize(2);
                    // 결과 두 개가 종료 알림보다 먼저고, 서버는 요청 둘을 받은 뒤에 종료 알림을 받는다.
                    assertThat(clients.get(i).log())
                            .as("클라이언트 %d의 기록", i)
                            .containsExactly(Peer.RESULT, Peer.RESULT, Peer.CLOSED);
                    assertThat(serverSides.get(i).log())
                            .as("서버 쪽 연결 %d의 기록", i)
                            .containsExactly(Peer.REQUEST_RECEIVED, Peer.REQUEST_RECEIVED, Peer.CLOSED);
                }
            }
        } finally {
            for (Peer client : clients) {
                client.stop();
            }
        }
    }
}
