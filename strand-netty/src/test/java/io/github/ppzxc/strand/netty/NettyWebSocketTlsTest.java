package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.netty.internal.TestTls;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** TLS 위의 WebSocket(wss): 기존 {@code security(tls)}를 재사용하고 ALPN 없이 협상한다(규격 3.7절). */
class NettyWebSocketTlsTest {
    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private AppFixture tlsFixture() throws Exception {
        AppFixture fixture = AppFixture.tls();
        closeables.add(fixture);
        return fixture;
    }

    private AppFixture plainFixture() {
        AppFixture fixture = AppFixture.plain();
        closeables.add(fixture);
        return fixture;
    }

    private Listener listen(AppFixture fixture, AtomicReference<PreparationFailure> failure) throws Exception {
        NettyStrandServer.Builder builder = fixture.serverBuilder()
                .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                .handler(request -> request.respond(Status.OK, request.payload()));
        if (failure != null) {
            builder.onConnectionFailure(failure::set);
        }
        Listener listener = AppFixture.get(builder.build().listen(AppFixture.anyLoopback(), Transport.webSocket("/s")));
        closeables.add(listener::close);
        return listener;
    }

    @Test
    void tlsWebSocketServerAndClientExchangeRequestsWithoutAlpn() throws Exception {
        AppFixture fixture = tlsFixture();
        Listener listener = listen(fixture, null);

        NettyStrandConnection connection = AppFixture.get(
                fixture.clientBuilder().build().connect(AppFixture.loopback(listener), Transport.webSocket("/s")));
        closeables.add(connection::abort);
        RequestResult result = fixture.result(
                connection.request(Request.of(TYPE, new byte[] {4, 2})).result());
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (result.status() == Status.UNAVAILABLE && System.nanoTime() < limit) {
            Thread.sleep(10);
            result = fixture.result(
                    connection.request(Request.of(TYPE, new byte[] {4, 2})).result());
        }

        assertThat(result.status()).isEqualTo(Status.OK);
        assertThat(result.payload()).containsExactly(4, 2);
    }

    @Test
    void aTlsWebSocketServerDoesNotNegotiateAlpnWithAClientThatOffersIt() throws Exception {
        AppFixture fixture = tlsFixture();
        Listener listener = listen(fixture, null);
        SSLContext context = TestTls.client();

        try (SSLSocket socket = (SSLSocket) context.getSocketFactory()
                .createSocket(
                        InetAddress.getLoopbackAddress(),
                        AppFixture.loopback(listener).getPort())) {
            socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS));
            SSLParameters parameters = socket.getSSLParameters();
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
            offerAlpn(parameters);
            socket.setSSLParameters(parameters);
            socket.startHandshake();

            assertThat(negotiated(socket)).isNullOrEmpty();
            String key = RawWebSocket.newKey();
            socket.getOutputStream()
                    .write(RawWebSocket.upgradeRequest("/s", "localhost", key, "strand")
                            .getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            assertThat(RawWebSocket.readHeaders(socket.getInputStream())).startsWith("HTTP/1.1 101");
        }
    }

    /** ALPN API는 Java 8(8u252 이전)에 없으므로 리플렉션으로 부른다. */
    private static void offerAlpn(SSLParameters parameters) throws Exception {
        SSLParameters.class.getMethod("setApplicationProtocols", String[].class).invoke(parameters, (Object)
                new String[] {"strand"});
    }

    private static String negotiated(SSLSocket socket) throws Exception {
        return (String) SSLSocket.class.getMethod("getApplicationProtocol").invoke(socket);
    }

    @Test
    void aPlaintextClientAgainstATlsServerFailsPreparationOnBothSides() throws Exception {
        AppFixture tls = tlsFixture();
        AtomicReference<PreparationFailure> serverFailure = new AtomicReference<>();
        Listener listener = listen(tls, serverFailure);
        AppFixture plain = plainFixture();

        ExecutionException failure = org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class,
                () -> AppFixture.get(plain.clientBuilder()
                        .preparationDeadline(Duration.ofSeconds(5))
                        .build()
                        .connect(AppFixture.loopback(listener), Transport.webSocket("/s"))));

        assertThat(failure.getCause()).isInstanceOf(PreparationFailedException.class);
        NettyWebSocketTest.awaitNotNull(serverFailure);
        assertThat(serverFailure.get().stage()).isEqualTo(PreparationStage.SECURITY);
    }

    @Test
    void aTlsClientAgainstAPlaintextServerFailsPreparationOnBothSides() throws Exception {
        AppFixture plain = plainFixture();
        AtomicReference<PreparationFailure> serverFailure = new AtomicReference<>();
        Listener listener = listen(plain, serverFailure);
        AppFixture tls = tlsFixture();

        ExecutionException failure = org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class,
                () -> AppFixture.get(tls.clientBuilder()
                        .preparationDeadline(Duration.ofSeconds(5))
                        .build()
                        .connect(AppFixture.loopback(listener), Transport.webSocket("/s"))));

        PreparationFailure clientFailure = ((PreparationFailedException) failure.getCause()).failure();
        assertThat(clientFailure.stage()).isEqualTo(PreparationStage.SECURITY);
        NettyWebSocketTest.awaitNotNull(serverFailure);
        assertThat(serverFailure.get().stage()).isEqualTo(PreparationStage.UPGRADE);
    }

    @Test
    void leavingSecurityUnsetIsTheSameConfigurationErrorAsForTcp() throws Exception {
        AppFixture fixture = plainFixture();

        assertThatThrownBy(() -> NettyStrandServer.builder()
                        .eventLoopGroup(fixture.group)
                        .executor(fixture.executor)
                        .resourceLimits(fixture.limits)
                        .preparationDeadline(Duration.ofSeconds(1))
                        .shutdownGrace(Duration.ofSeconds(1))
                        .shutdownDeadline(Duration.ofSeconds(2))
                        .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                        .handler(request -> {})
                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("security");
    }
}
