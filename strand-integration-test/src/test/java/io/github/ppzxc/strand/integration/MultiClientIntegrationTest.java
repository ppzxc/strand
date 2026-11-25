package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.nio.internal.ControlledPassage;
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
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(PassageObservation.class)
class MultiClientIntegrationTest {

    private static final int CLIENT_COUNT = 16;

    private static final long FIRST_CLIENT_STREAM_ID = 1L;

    private static TcpServer startServer(Transport transport) throws Exception {
        return TcpServer.start(transport, type -> type - APPLICATION_TYPE);
    }

    private static CompletableFuture<RequestResult> requestFor(Peer client, int index) {
        Peer.Sent sent = client.request(APPLICATION_TYPE + index, Fixtures.payload("client-" + index));
        client.logResult(sent);
        return sent.result();
    }

    private static RequestResult await(CompletableFuture<RequestResult> future) throws Exception {
        return future.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @TransportTest
    @DisplayName("클라이언트 16개가 동시에 연결해도 응답이 자기 연결로만 돌아온다 (규격 4.2)")
    void concurrentClientsEachGetTheirOwnResponse(Transport transport) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(CLIENT_COUNT);
        try (TcpServer server = startServer(transport)) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<RequestResult>> results = new ArrayList<>();
            for (int i = 0; i < CLIENT_COUNT; i++) {
                int index = i;
                results.add(pool.submit(() -> connectAndRequest(transport, server.port(), index, start)));
            }
            start.countDown();

            for (int i = 0; i < CLIENT_COUNT; i++) {
                RequestResult result = results.get(i).get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
                assertThat(result.certainty()).as("클라이언트 %d의 결과 확정성", i).isEqualTo(Certainty.RESPONSE);
                assertThat(result.status()).as("클라이언트 %d의 status", i).isEqualTo(i);
            }

            if (!transport.observesPassage()) {
                return;
            }

            Map<Integer, Long> streamByType = new HashMap<>();
            for (int i = 0; i < CLIENT_COUNT; i++) {
                Peer serverSide = server.awaitPeer();
                serverSide.awaitRequest();
                for (ControlledPassage.Frame frame : serverSide.passage().readFrames()) {
                    if (frame.type() >= APPLICATION_TYPE) {
                        streamByType.put(frame.type(), frame.streamId());
                    }
                }
            }
            assertThat(streamByType)
                    .hasSize(CLIENT_COUNT)
                    .allSatisfy((type, streamId) -> assertThat(streamId).isEqualTo(FIRST_CLIENT_STREAM_ID));
        } finally {
            pool.shutdownNow();
        }
    }

    private static RequestResult connectAndRequest(Transport transport, int port, int index, CountDownLatch start)
            throws Exception {
        start.await();
        try (Peer client = Peer.connect(transport, port, null)) {
            return await(requestFor(client, index));
        }
    }

    @TransportTest
    @DisplayName("수립된 연결 하나가 비정상 종료돼도 나머지 연결은 영향을 받지 않는다 (규격 3.5, 4.2)")
    void oneAbruptDisconnectDoesNotAffectOthers(Transport transport) throws Exception {
        int aborted = 0;
        List<Peer> clients = new ArrayList<>();
        List<Peer> serverSides = new ArrayList<>();
        try (TcpServer server = startServer(transport)) {
            for (int i = 0; i < CLIENT_COUNT; i++) {
                clients.add(Peer.connect(transport, server.port(), null));

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
            CloseNotice abortedNotice = serverSides.get(aborted).awaitClosed();
            for (int i = 0; i < second.size(); i++) {
                int client = i < aborted ? i : i + 1;
                assertThat(await(second.get(i)).status()).isEqualTo(client);
            }

            for (int i = 0; i < CLIENT_COUNT; i++) {
                if (i != aborted) {
                    clients.get(i).closeConnection();
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

            assertThat(abortedNotice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
            assertThat(serverSides.get(aborted).closedNotices()).hasSize(1);
            assertThat(serverSides.get(aborted).drainRequests()).hasSize(1);
            assertThat(serverSides.get(aborted).log()).containsExactly(Peer.REQUEST_RECEIVED, Peer.CLOSED);
            for (int i = 0; i < CLIENT_COUNT; i++) {
                if (i != aborted) {
                    assertThat(clients.get(i).closedNotices())
                            .as("클라이언트 %d의 종료 알림", i)
                            .hasSize(1);
                    assertThat(clients.get(i).closedNotices().get(0).kind()).isEqualTo(CloseNotice.Kind.CLOSE);
                    assertThat(serverSides.get(i).closedNotices())
                            .as("서버 쪽 연결 %d의 종료 알림", i)
                            .hasSize(1);
                    assertThat(serverSides.get(i).closedNotices().get(0).kind()).isEqualTo(CloseNotice.Kind.CLOSE);
                    assertThat(serverSides.get(i).drainRequests())
                            .as("서버 쪽 연결 %d의 받은 요청", i)
                            .hasSize(2);

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
