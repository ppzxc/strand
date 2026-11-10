package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.CLOSE_FRAME_BYTES;
import static io.github.ppzxc.strand.integration.Fixtures.CLOSE_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.nio.internal.ConnectionClosed;
import io.github.ppzxc.strand.nio.internal.IncomingRequest;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import io.github.ppzxc.strand.nio.internal.TcpPassage;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
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

    private static RequestResult await(CompletableFuture<RequestResult> future) throws Exception {
        return future.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * {@code NO_ERROR} CLOSE는 대기 중인 스트림이 끝날 때까지 연결을 닫지 않는다. 서버가 응답을 보류한 채 클라이언트가 CLOSE를 보내고, 그 뒤에 서버가
     * 응답한다. 대기 중인 요청을 버리고 바로 닫는 구현은 클라이언트가 {@code OK}가 아니라 로컬 {@code UNAVAILABLE}을 받아 실패한다. 종료 알림이 마지막
     * 스트림의 결과보다 먼저 와도 실패하므로 알림을 받았을 때 결과 Future가 이미 끝나 있는지도 함께 본다. 서버가 CLOSE를 처리했는지는 받은 바이트 수라는
     * 관찰 지점으로 본다. 서버가 처리한 바이트 수가 CLOSE 프레임 크기만큼 늘어난 것을 확인한 뒤에 응답한다. 응답이 CLOSE보다 먼저 닿는 경로로 통과하지
     * 않게 하려는 것이다.
     */
    @Test
    @DisplayName("대기 요청이 있는 채 NO_ERROR CLOSE를 보내도 응답을 받은 뒤에 닫힌다 (규격 3.5)")
    void noErrorCloseWaitsForPendingResponse() throws Exception {
        try (TcpServer server = TcpServer.start();
                Peer client = Peer.connect(server.port(), null)) {
            Peer serverSide = server.awaitPeer();
            CompletableFuture<RequestResult> result =
                    client.request(APPLICATION_TYPE, Fixtures.payload("hello")).result();
            IncomingRequest received = serverSide.awaitRequest();
            long receivedBeforeClose = serverSide.driver().receivedBytes();

            client.driver().closeConnection();
            serverSide.awaitReceivedBytes(receivedBeforeClose + CLOSE_FRAME_BYTES);
            assertThat(result).isNotDone();
            received.respond(STATUS_OK, Fixtures.payload("late")).get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
            ConnectionClosed atClient = client.awaitClosed();
            ConnectionClosed atServer = serverSide.awaitClosed();

            assertThat(result).isDone();
            assertThat(await(result).status()).isEqualTo(STATUS_OK);
            assertThat(await(result).certainty()).isEqualTo(Certainty.RESPONSE);
            assertThat(atClient.reason().kind()).isEqualTo(CloseReason.Kind.CLOSE);
            assertThat(atServer.reason().kind()).isEqualTo(CloseReason.Kind.CLOSE);
        }
    }

    /**
     * 양쪽에 대기 중인 스트림이 있으면 {@code NO_ERROR} CLOSE를 보내도 연결은 닫히지 않고 종료 중이 된다. 이 상태에서 상대의 CLOSE가 오면(엇갈림) 이미 CLOSE를
     * 보냈으므로 답하는 CLOSE를 다시 보내서는 안 된다(규격 3.5절 "CLOSE를 보냈거나 받은 뒤에는 CLOSE를 보내지 않는다(MUST NOT)", "양쪽이 동시에 CLOSE를 보내
     * 엇갈리는 것은 허용한다"). 서로 요청을 낸 뒤 응답을 보류하고, 두 통로의 쓰기를 붙잡아 양쪽이 CLOSE를 보낸 것을 확인한 뒤에야 둘을 함께 풀어 서로의 CLOSE를
     * 보기 전에 각자 CLOSE를 보내게 한다. 둘 다 상대 CLOSE를 처리한 뒤 서로 응답해 끝내고, 통로에 쓴 CLOSE 프레임이 각자 하나뿐인지 본다. 두 번째 CLOSE를
     * 보내면 CLOSE 프레임이 둘이 되고, 종료 알림이 중복되면 알림 수가 둘이 된다. 대기 스트림이 없으면 CLOSE를 보내는 즉시 닫혀 상대 CLOSE를 처리하지 않으므로 이
     * 경로를 시험할 수 없다.
     */
    @Test
    @DisplayName("대기 스트림이 있는 양쪽이 서로의 CLOSE를 보기 전에 CLOSE를 보내도 CLOSE는 각자 하나뿐이다 (규격 3.5)")
    void crossedCloseWhileClosingSendsOneCloseEach() throws Exception {
        try (TcpServer server = TcpServer.start(
                null, channel -> new ControlledPassage(Peer.tcp(channel, TcpPassage.DEFAULT_READ_BUFFER_BYTES)))) {
            ControlledPassage clientPassage = new ControlledPassage(TcpPassage.of(Peer.open(server.port())));
            try (Peer client = Peer.client(clientPassage, null)) {
                Peer serverSide = server.awaitPeer();
                ControlledPassage serverPassage = serverSide.passage();
                CompletableFuture<RequestResult> fromClient = client.request(
                                APPLICATION_TYPE, Fixtures.payload("from-client"))
                        .result();
                CompletableFuture<RequestResult> fromServer = serverSide
                        .request(APPLICATION_TYPE, Fixtures.payload("from-server"))
                        .result();
                IncomingRequest requestAtServer = serverSide.awaitRequest();
                IncomingRequest requestAtClient = client.awaitRequest();
                long clientFramesBefore = client.driver().writtenFrames();
                long serverFramesBefore = serverSide.driver().writtenFrames();
                long clientReceivedBefore = client.driver().receivedBytes();
                long serverReceivedBefore = serverSide.driver().receivedBytes();

                clientPassage.deferWrites();
                serverPassage.deferWrites();
                client.driver().closeConnection();
                serverSide.driver().closeConnection();
                Peer.awaitUntil(
                        () -> client.driver().writtenFrames() > clientFramesBefore
                                && serverSide.driver().writtenFrames() > serverFramesBefore,
                        "양쪽이 CLOSE를 쓰지 않았다");
                clientPassage.releaseWrites();
                serverPassage.releaseWrites();
                client.awaitReceivedBytes(clientReceivedBefore + CLOSE_FRAME_BYTES);
                serverSide.awaitReceivedBytes(serverReceivedBefore + CLOSE_FRAME_BYTES);

                requestAtServer
                        .respond(STATUS_OK, Fixtures.payload("ok"))
                        .get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
                requestAtClient
                        .respond(STATUS_OK, Fixtures.payload("ok"))
                        .get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
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

    /**
     * 대기 스트림이 없는 양쪽이 동시에 {@code NO_ERROR} CLOSE를 보내면 각자 CLOSE를 보내는 즉시 닫히고, 뒤따르는 상대 CLOSE와 TCP 끊김은 무시되어야
     * 한다. 그래도 종료 알림은 양쪽에서 정확히 한 번씩이어야 한다(규격 3.5절 "연결당 정확히 한 번"). 중복되면 연결을 닫은 뒤의 알림 수가 둘이 된다. 실제
     * 타이밍에서 양쪽이 엇갈리는지는 보장되지 않으므로 시작 신호로 동시에 출발시켜 {@value #REPEATS}번 되풀이한다. 엇갈림을 확정해서 보는 시험은 {@link
     * #crossedCloseWhileClosingSendsOneCloseEach()}가 맡는다.
     */
    @Test
    @DisplayName("양쪽이 동시에 CLOSE를 보내도 종료 알림은 양쪽에서 한 번씩만 온다 (규격 3.5)")
    void simultaneousCloseNotifiesOncePerSide() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (TcpServer server = TcpServer.start(type -> STATUS_OK)) {
            for (int round = 0; round < REPEATS; round++) {
                try (Peer client = Peer.connect(server.port(), null)) {
                    Peer serverSide = server.awaitPeer();
                    await(client.request(APPLICATION_TYPE, Fixtures.payload("hello"))
                            .result());

                    CountDownLatch start = new CountDownLatch(1);
                    Future<?> clientClose = pool.submit(() -> {
                        start.await();
                        client.driver().closeConnection();
                        return null;
                    });
                    Future<?> serverClose = pool.submit(() -> {
                        start.await();
                        serverSide.driver().closeConnection();
                        return null;
                    });
                    start.countDown();
                    clientClose.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    serverClose.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    client.awaitClosed();
                    serverSide.awaitClosed();
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
