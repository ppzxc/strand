package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NettyWebSocketLifecycleTest {
    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private AppFixture fixture() {
        AppFixture fixture = AppFixture.plain();
        closeables.add(fixture);
        return fixture;
    }

    private static RequestResult requestWhenReady(AppFixture fixture, NettyStrandConnection connection, byte[] body)
            throws Exception {
        RequestResult result =
                fixture.result(connection.request(Request.of(TYPE, body)).result());
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (result.status() == Status.UNAVAILABLE && System.nanoTime() < limit) {
            Thread.sleep(10);
            result = fixture.result(connection.request(Request.of(TYPE, body)).result());
        }
        return result;
    }

    private ServerSocket silentServer() throws IOException {
        ServerSocket server = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
        closeables.add(server);
        Thread accept = new Thread(
                () -> {
                    try {
                        for (; ; ) {
                            Socket socket = server.accept();
                            closeables.add(socket);
                        }
                    } catch (IOException e) {
                    }
                },
                "silent-server");
        accept.setDaemon(true);
        accept.start();
        return server;
    }

    @Test
    void authenticationSucceedsAndFailsLikeTcp() throws Exception {
        AppFixture fixture = fixture();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> "OK".equals(credentials.mechanism())
                        ? AuthenticationResult.accepted("alice")
                        : AuthenticationResult.rejected())
                .handler(request -> request.respond(Status.OK, request.payload()))
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback(), Transport.webSocket()));
        closeables.add(listener::close);

        NettyStrandConnection accepted = AppFixture.get(fixture.clientBuilder()
                .credentials(Credentials.of("OK", new byte[] {1}))
                .build()
                .connect(AppFixture.loopback(listener), Transport.webSocket()));
        closeables.add(accepted::abort);
        ExecutionException rejected = org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class,
                () -> AppFixture.get(fixture.clientBuilder()
                        .credentials(Credentials.of("BAD", new byte[] {1}))
                        .build()
                        .connect(AppFixture.loopback(listener), Transport.webSocket())));
        assertThat(requestWhenReady(fixture, accepted, new byte[] {1}).status()).isEqualTo(Status.OK);
        assertThat(rejected.getCause()).isInstanceOf(PreparationFailedException.class);
        assertThat(((PreparationFailedException) rejected.getCause()).failure().stage())
                .isEqualTo(PreparationStage.AUTHENTICATION);
    }

    @Test
    void aConnectionStuckInTheUpgradeEndsByThePreparationDeadline() throws Exception {
        AppFixture fixture = fixture();
        AtomicReference<PreparationFailure> failure = new AtomicReference<>();
        NettyStrandServer server = fixture.serverBuilder()
                .preparationDeadline(Duration.ofMillis(400))
                .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                .handler(request -> {})
                .onConnectionFailure(failure::set)
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback(), Transport.webSocket("/s")));
        closeables.add(listener::close);

        try (Socket socket = new Socket(
                InetAddress.getLoopbackAddress(), AppFixture.loopback(listener).getPort())) {
            socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS));
            socket.getOutputStream().write("GET /s HTTP/1.1\r\nHost: x\r\n".getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();

            assertThat(socket.getInputStream().read()).as("서버가 기한이 지나 연결을 끊는다").isEqualTo(-1);
        }
        NettyWebSocketTest.awaitNotNull(failure);
        assertThat(failure.get().stage()).isEqualTo(PreparationStage.UPGRADE);
        assertThat(failure.get().cause()).isInstanceOf(PreparationDeadlineExceededException.class);
    }

    @Test
    void theClientPreparationDeadlineCoversTheUpgrade() throws Exception {
        AppFixture fixture = fixture();
        ServerSocket silent = silentServer();

        ExecutionException failure = org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class,
                () -> AppFixture.get(fixture.clientBuilder()
                        .preparationDeadline(Duration.ofMillis(400))
                        .build()
                        .connect(
                                new InetSocketAddress(InetAddress.getLoopbackAddress(), silent.getLocalPort()),
                                Transport.webSocket())));

        PreparationFailure preparation = ((PreparationFailedException) failure.getCause()).failure();
        assertThat(preparation.stage()).isEqualTo(PreparationStage.UPGRADE);
        assertThat(preparation.cause()).isInstanceOf(PreparationDeadlineExceededException.class);
    }

    @Test
    void cancellingDuringTheUpgradeEndsTheAttemptAndReleasesTheChannel() throws Exception {
        AppFixture fixture = fixture();
        ServerSocket silent = silentServer();
        NettyConnectAttempt attempt = fixture.clientBuilder()
                .build()
                .connectAttempt(
                        new InetSocketAddress(InetAddress.getLoopbackAddress(), silent.getLocalPort()),
                        Transport.webSocket("/s"));
        Thread.sleep(300);

        assertThat(attempt.cancel()).isTrue();

        ExecutionException failure = org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class, () -> AppFixture.get(attempt.result()));
        PreparationFailure preparation = ((PreparationFailedException) failure.getCause()).failure();
        assertThat(preparation.cause()).isInstanceOf(ConnectionAttemptCancelledException.class);
        AppFixture.get(attempt.transportReleased());
    }

    @Test
    void oneServerRunsTcpAndWebSocketListenersAndShutdownClosesBoth() throws Exception {
        AppFixture fixture = fixture();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                .handler(request -> request.respond(Status.OK, request.payload()))
                .build();
        Listener tcp = AppFixture.get(server.listen(AppFixture.anyLoopback(), Transport.tcp()));
        Listener webSocket = AppFixture.get(server.listen(AppFixture.anyLoopback(), Transport.webSocket("/s")));
        NettyStrandClient client = fixture.clientBuilder().build();

        NettyStrandConnection overTcp = AppFixture.get(client.connect(AppFixture.loopback(tcp), Transport.tcp()));
        NettyStrandConnection overWs =
                AppFixture.get(client.connect(AppFixture.loopback(webSocket), Transport.webSocket("/s")));

        assertThat(requestWhenReady(fixture, overTcp, new byte[] {1}).payload()).containsExactly(1);
        assertThat(requestWhenReady(fixture, overWs, new byte[] {2}).payload()).containsExactly(2);
        assertThat(server.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isEqualTo(2);

        AppFixture.get(server.shutdown());
        AppFixture.get(server.resourcesTerminated());

        AppFixture.get(tcp.closed());
        AppFixture.get(webSocket.closed());
        assertThat(server.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        CompletableFuture<?> bothEnded =
                CompletableFuture.allOf(fixture.resultFuture(overTcp.closed()), fixture.resultFuture(overWs.closed()));
        bothEnded.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        AppFixture.get(client.shutdown());
    }

    @Test
    void aWebSocketPortDoesNotAnswerATcpClientAndViceVersa() throws Exception {
        AppFixture fixture = fixture();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                .handler(request -> {})
                .build();
        Listener tcp = AppFixture.get(server.listen(AppFixture.anyLoopback(), Transport.tcp()));
        closeables.add(tcp::close);
        Listener webSocket = AppFixture.get(server.listen(AppFixture.anyLoopback(), Transport.webSocket("/s")));
        closeables.add(webSocket::close);
        NettyStrandClient client = fixture.clientBuilder()
                .preparationDeadline(Duration.ofSeconds(3))
                .build();

        org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class,
                () -> AppFixture.get(client.connect(AppFixture.loopback(webSocket), Transport.tcp())));
        org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class,
                () -> AppFixture.get(client.connect(AppFixture.loopback(tcp), Transport.webSocket("/s"))));
    }
}
