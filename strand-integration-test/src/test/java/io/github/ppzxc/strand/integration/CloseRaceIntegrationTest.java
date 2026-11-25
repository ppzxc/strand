package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.CLOSE_FRAME_BYTES;
import static io.github.ppzxc.strand.integration.Fixtures.CLOSE_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(PassageObservation.class)
class CloseRaceIntegrationTest {

    private static final int REPEATS = 20;

    private static final long WELL_BEFORE_FINISH_DEADLINE_MS = 2_000;

    private static RequestResult await(CompletableFuture<RequestResult> future) throws Exception {
        return future.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Test
    @DisplayName("[nio 전용] 대기 요청이 있는 채 NO_ERROR CLOSE를 보내도 응답을 받은 뒤에 닫힌다 (규격 3.5)")
    void noErrorCloseWaitsForPendingResponse() throws Exception {
        Transport transport = Transport.NIO;
        try (TcpServer server = TcpServer.start(transport);
                Peer client = Peer.connect(transport, server.port(), null)) {
            Peer serverSide = server.awaitPeer();
            Peer.Sent sent = client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
            CompletableFuture<RequestResult> result = sent.result();
            Wire.Incoming received = serverSide.awaitRequest();
            long receivedBeforeClose = serverSide.passage().readBytes();

            client.closeConnection();
            serverSide.awaitReceivedBytes(receivedBeforeClose + CLOSE_FRAME_BYTES);
            assertThat(result).isNotDone();
            Fixtures.await(received.respond(STATUS_OK, Fixtures.payload("late")));
            CloseNotice atClient = client.awaitClosed();
            CloseNotice atServer = serverSide.awaitClosed();

            assertThat(sent.isResultRetained()).isTrue();
            assertThat(await(result).status()).isEqualTo(STATUS_OK);
            assertThat(await(result).certainty()).isEqualTo(Certainty.RESPONSE);
            assertThat(atClient.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
            assertThat(atServer.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        }
    }

    @Test
    @DisplayName("[nio 전용] 대기 스트림이 있는 양쪽이 서로의 CLOSE를 보기 전에 CLOSE를 보내도 CLOSE는 각자 하나뿐이다 (규격 3.5)")
    void crossedCloseWhileClosingSendsOneCloseEach() throws Exception {
        Transport transport = Transport.NIO;
        try (TcpServer server = TcpServer.start(transport)) {
            try (Peer client = Peer.connect(transport, server.port(), null)) {
                Peer serverSide = server.awaitPeer();
                ControlledPassage clientPassage = client.passage();
                ControlledPassage serverPassage = serverSide.passage();
                CompletableFuture<RequestResult> fromClient = client.request(
                                APPLICATION_TYPE, Fixtures.payload("from-client"))
                        .result();
                CompletableFuture<RequestResult> fromServer = serverSide
                        .request(APPLICATION_TYPE, Fixtures.payload("from-server"))
                        .result();
                Wire.Incoming requestAtServer = serverSide.awaitRequest();
                Wire.Incoming requestAtClient = client.awaitRequest();
                long clientFramesBefore = clientPassage.writtenFrames().size();
                long serverFramesBefore = serverPassage.writtenFrames().size();
                long clientReceivedBefore = clientPassage.readBytes();
                long serverReceivedBefore = serverPassage.readBytes();

                clientPassage.deferWrites();
                serverPassage.deferWrites();
                client.closeConnection();
                serverSide.closeConnection();
                Peer.awaitUntil(
                        () -> clientPassage.writtenFrames().size() > clientFramesBefore
                                && serverPassage.writtenFrames().size() > serverFramesBefore,
                        "양쪽이 CLOSE를 쓰지 않았다");
                clientPassage.releaseWrites();
                serverPassage.releaseWrites();
                client.awaitReceivedBytes(clientReceivedBefore + CLOSE_FRAME_BYTES);
                serverSide.awaitReceivedBytes(serverReceivedBefore + CLOSE_FRAME_BYTES);

                Fixtures.await(requestAtServer.respond(STATUS_OK, Fixtures.payload("ok")));
                Fixtures.await(requestAtClient.respond(STATUS_OK, Fixtures.payload("ok")));
                client.awaitClosed();
                serverSide.awaitClosed();
                client.stop();
                serverSide.stop();

                assertThat(await(fromClient).certainty()).isEqualTo(Certainty.RESPONSE);
                assertThat(await(fromServer).certainty()).isEqualTo(Certainty.RESPONSE);
                assertThat(Collections.frequency(clientPassage.writtenFrameTypes(), CLOSE_TYPE))
                        .isEqualTo(1);
                assertThat(Collections.frequency(serverPassage.writtenFrameTypes(), CLOSE_TYPE))
                        .isEqualTo(1);
                assertThat(client.closedNotices()).hasSize(1);
                assertThat(serverSide.closedNotices()).hasSize(1);
            }
        }
    }

    @TransportTest
    @DisplayName("양쪽이 동시에 CLOSE를 보내도 종료 알림은 양쪽에서 한 번씩만 온다 (규격 3.5)")
    void simultaneousCloseNotifiesOncePerSide(Transport transport) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (TcpServer server = TcpServer.start(transport, type -> STATUS_OK)) {
            for (int round = 0; round < REPEATS; round++) {
                try (Peer client = Peer.connect(transport, server.port(), null)) {
                    Peer serverSide = server.awaitPeer();
                    await(client.request(APPLICATION_TYPE, Fixtures.payload("hello"))
                            .result());

                    CountDownLatch start = new CountDownLatch(1);
                    long startedAt = System.nanoTime();
                    Future<?> clientClose = pool.submit(() -> {
                        start.await();
                        client.closeConnection();
                        return null;
                    });
                    Future<?> serverClose = pool.submit(() -> {
                        start.await();
                        serverSide.closeConnection();
                        return null;
                    });
                    start.countDown();
                    clientClose.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    serverClose.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    client.awaitClosed();
                    serverSide.awaitClosed();
                    assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt))
                            .as("%d번째 종료가 마무리 기한을 기다리지 않고 끝난다", round)
                            .isLessThan(WELL_BEFORE_FINISH_DEADLINE_MS);
                    client.stop();
                    serverSide.stop();

                    assertThat(client.closedNotices())
                            .as("%d번째 클라이언트 알림", round)
                            .hasSize(1);
                    assertThat(serverSide.closedNotices())
                            .as("%d번째 서버 쪽 알림", round)
                            .hasSize(1);
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
