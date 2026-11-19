package io.github.ppzxc.strand.nio;

import static io.github.ppzxc.strand.nio.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Status;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** listener.close()/closed()는 리스닝·경합 수락 소켓만 정리하고 이미 등록된 준비·활성 연결을 유지한다(#320). */
class NioListenerCloseTest {

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private AppFixture track(AppFixture fixture) {
        closeables.add(fixture);
        return fixture;
    }

    private NioStrandServer.Builder acceptingServer(AppFixture fixture) {
        return fixture.serverBuilder().authenticate(c -> AuthenticationResult.accepted("p"));
    }

    private Listener listen(NioStrandServer server) throws Exception {
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        return listener;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void closedIsNotDoneBeforeCloseAndCompletesAfterTheListeningSocketIsReleased() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        Listener listener = listen(acceptingServer(fixture).handler(r -> {}).build());
        int port = ((InetSocketAddress) listener.localAddress()).getPort();

        assertThat(listener.closed().toCompletableFuture().isDone()).isFalse();
        listener.close();
        AppFixture.get(listener.closed());

        // 리스닝 소켓이 실제로 풀렸으므로 같은 포트에 다시 바인딩할 수 있다.
        try (ServerSocketChannel again = ServerSocketChannel.open()) {
            again.setOption(java.net.StandardSocketOptions.SO_REUSEADDR, true);
            again.bind(new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), port));
            assertThat(again.isOpen()).isTrue();
        }
    }

    @Test
    void closeIsIdempotentAndClosedKeepsTheSameCompletion() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        Listener listener = listen(acceptingServer(fixture).handler(r -> {}).build());

        listener.close();
        listener.close();
        AppFixture.get(listener.closed());
        listener.close();

        assertThat(listener.closed().toCompletableFuture().isDone()).isTrue();
    }

    @Test
    void registeredActiveConnectionsSurviveListenerCloseAndClosedDoesNotWaitForThem() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        Listener listener = listen(acceptingServer(server)
                .handler(request -> request.respond(Status.OK, bytes("still")))
                .build());
        NioStrandClient client = clientFixture.clientBuilder().build();
        NioStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        closeables.add(connection::abort);

        listener.close();
        AppFixture.get(listener.closed());

        assertThat(connection.closed().isDone()).isFalse();
        RequestResult result = clientFixture.requestWhenServed(connection, Request.of(TYPE, bytes("q")));
        assertThat(result.status()).isEqualTo(Status.OK);
        assertThat(new String(result.payload(), StandardCharsets.UTF_8)).isEqualTo("still");
    }

    @Test
    void aConnectionStillPreparingWhenTheListenerClosesIsKept() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch authStarted = new CountDownLatch(1);
        CompletableFuture<Void> finishAuth = new CompletableFuture<>();
        Listener listener = listen(server.serverBuilder()
                .authenticate(c -> {
                    authStarted.countDown();
                    finishAuth.get(30, TimeUnit.SECONDS);
                    return AuthenticationResult.accepted("p");
                })
                .handler(r -> {})
                .build());
        NioStrandClient client = clientFixture.clientBuilder().build();
        CompletableFuture<NioStrandConnection> connecting =
                client.connect(AppFixture.loopback(listener)).toCompletableFuture();
        assertThat(authStarted.await(15, TimeUnit.SECONDS)).isTrue();

        listener.close();
        AppFixture.get(listener.closed());
        assertThat(connecting.isDone()).isFalse();
        finishAuth.complete(null);

        NioStrandConnection connection = connecting.get(15, TimeUnit.SECONDS);
        closeables.add(connection::abort);
        assertThat(connection.closed().isDone()).isFalse();
    }

    @Test
    void aNewConnectionAfterCloseIsNotAccepted() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        Listener listener = listen(acceptingServer(server).handler(r -> {}).build());
        listener.close();
        AppFixture.get(listener.closed());
        NioStrandClient client = clientFixture.clientBuilder().build();

        assertThatThrownBy(() -> AppFixture.get(client.connect(AppFixture.loopback(listener))))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PreparationFailedException.class);
    }

    @Test
    void closedWaitsForTheRacingAcceptedSocketToBeCleanedUp() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        CountDownLatch inAdmit = new CountDownLatch(1);
        CountDownLatch finishAdmit = new CountDownLatch(1);
        ServerSocketChannel server = ServerSocketChannel.open();
        closeables.add(server::close);
        server.bind(AppFixture.anyLoopback());
        Listener listener = new Listener(
                server,
                server.getLocalAddress(),
                accepted -> {
                    inAdmit.countDown();
                    try {
                        finishAdmit.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    PassageFactory.closeQuietly(accepted);
                },
                fixture.executor);
        closeables.add(listener::close);
        SocketChannel client = SocketChannel.open(server.getLocalAddress());
        closeables.add(client::close);
        assertThat(inAdmit.await(15, TimeUnit.SECONDS)).isTrue();

        listener.close();

        // 경합해 수락한 소켓의 등록이 끝나기 전에는 정리 완료가 아니다.
        assertThat(listener.closed().toCompletableFuture().isDone()).isFalse();
        finishAdmit.countDown();
        AppFixture.get(listener.closed());
    }
}
