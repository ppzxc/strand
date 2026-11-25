package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.ErrorCode;
import io.github.ppzxc.strand.core.internal.Event;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class NettyWebSocketCloseTest {
    private static final String PATH = "/strand";

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();
    private final CompletableFuture<NettyStrandConnection> accepted = new CompletableFuture<>();
    private AppFixture fixture;
    private int port;

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private void startServer(CredentialVerifier verifier, ConnectionHandlerFactory factory, Duration idle, Duration ack)
            throws Exception {
        fixture = AppFixture.plain();
        closeables.add(fixture);
        NettyStrandServer.Builder builder =
                fixture.serverBuilder().authenticate(verifier).handlerFactory(factory);
        if (idle != null) {
            builder.idlePingInterval(idle);
        }
        if (ack != null) {
            builder.ackTimeout(ack);
        }
        Listener listener = AppFixture.get(builder.build().listen(AppFixture.anyLoopback(), Transport.webSocket(PATH)));
        closeables.add(listener::close);
        port = AppFixture.loopback(listener).getPort();
    }

    private void startServer() throws Exception {
        startServer(
                credentials -> AuthenticationResult.accepted("peer"),
                connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> request.respond(Status.OK, request.payload()));
                },
                null,
                null);
    }

    private WsPeer peer() throws Exception {
        WsPeer peer = WsPeer.connect(port, PATH);
        closeables.add(peer);
        return peer;
    }

    private static void awaitOpen(WsPeer peer) throws Exception {
        WsPeer.await(() -> peer.events().stream().anyMatch(event -> event.kind() == Event.Kind.AUTHENTICATED));
    }

    private static void awaitWebSocketClose(WsPeer peer) throws Exception {
        WsPeer.await(() -> !peer.closes().isEmpty());
    }

    private static int status(WsPeer peer) {
        return peer.closes().get(0).closeStatus();
    }

    private static String lastArrival(WsPeer peer) {
        List<String> arrivals = peer.arrivals();
        return arrivals.get(arrivals.size() - 1);
    }

    @Test
    void normalCloseEndsWithConnectionCloseThenWebSocketClose1000() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);
        NettyStrandConnection connection = accepted.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);

        connection.close();

        awaitWebSocketClose(peer);
        assertThat(status(peer)).isEqualTo(1000);
        assertThat(peer.closes().get(0).closeReason()).isEmpty();
        List<String> arrivals = peer.arrivals();
        assertThat(arrivals.indexOf("CLOSE:1000")).isGreaterThan(arrivals.lastIndexOf("BINARY"));
        assertThat(peer.events())
                .anyMatch(event -> event.kind() == Event.Kind.CLOSED
                        && event.closeReason().get().errorCode() == ErrorCode.NO_ERROR);
    }

    @Test
    void remainingOutputIsSentBeforeTheWebSocketClose() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);
        NettyStrandConnection connection = accepted.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        int before = peer.binaryMessages().size();

        for (int i = 0; i < 4; i++) {
            connection.request(Request.of(AppFixture.TYPE, new byte[64 * 1024]));
        }
        connection.close();

        awaitWebSocketClose(peer);
        assertThat(lastArrival(peer)).isEqualTo("CLOSE:1000");
        assertThat(peer.binaryMessages().size()).isGreaterThan(before);
    }

    @ParameterizedTest
    @CsvSource({
        "PROTOCOL_ERROR, 1002",
        "EXCESSIVE_USE, 1008",
        "INTERNAL_ERROR, 1011",
        "UNRESPONSIVE, 1011",
        "UNAUTHENTICATED, 1008",
    })
    void aPeerSentErrorCloseUsesTheSameTable(String code, int expectedStatus) throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);
        int errorCode = errorCodeOf(code);

        peer.run(endpoint -> endpoint.close(errorCode));

        awaitWebSocketClose(peer);
        assertThat(status(peer)).isEqualTo(expectedStatus);
        assertThat(status(peer)).isNotEqualTo(1000);
        assertThat(peer.closes().get(0).closeReason()).isEmpty();
    }

    private static int errorCodeOf(String name) {
        switch (name) {
            case "PROTOCOL_ERROR":
                return ErrorCode.PROTOCOL_ERROR;
            case "EXCESSIVE_USE":
                return ErrorCode.EXCESSIVE_USE;
            case "INTERNAL_ERROR":
                return ErrorCode.INTERNAL_ERROR;
            case "UNRESPONSIVE":
                return ErrorCode.UNRESPONSIVE;
            default:
                return ErrorCode.UNAUTHENTICATED;
        }
    }

    @Test
    void excessiveUseCloseSentByTheServerUses1008() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);
        NettyStrandConnection connection = accepted.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);

        connection.closeExcessiveUse();

        awaitWebSocketClose(peer);
        assertThat(status(peer)).isEqualTo(1008);
        assertThat(peer.closes().get(0).closeReason()).isEmpty();
    }

    @Test
    void rejectedAuthenticationClosesWith1008() throws Exception {
        startServer(
                credentials -> AuthenticationResult.rejected(),
                connection -> PreparedHandler.of(request -> request.respond(Status.OK, request.payload())),
                null,
                null);
        WsPeer peer = peer();

        awaitWebSocketClose(peer);

        assertThat(status(peer)).isEqualTo(1008);
        assertThat(peer.events())
                .anyMatch(event -> event.kind() == Event.Kind.CLOSED
                        && event.closeReason().get().errorCode() == ErrorCode.UNAUTHENTICATED);
    }

    @Test
    void anUnresponsivePeerClosesWith1011() throws Exception {
        startServer(
                credentials -> AuthenticationResult.accepted("peer"),
                connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> request.respond(Status.OK, request.payload()));
                },
                Duration.ofMillis(100),
                Duration.ofMillis(200));
        WsPeer peer = peer();
        awaitOpen(peer);

        peer.goSilent();

        awaitWebSocketClose(peer);
        assertThat(status(peer)).isEqualTo(1011);
        NettyStrandConnection connection = accepted.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        CloseNotice notice = fixture.result(connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
    }

    @Test
    void closeWithoutAConnectionCloseUses1002() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);

        peer.sendText("not binary");

        awaitWebSocketClose(peer);
        assertThat(status(peer)).isEqualTo(1002);
        assertThat(peer.closes().get(0).closeReason()).isEmpty();
    }

    @Test
    void aPrefaceErrorClosesWith1002() throws Exception {
        startServer();
        WsPeer peer = WsPeer.connect(port, PATH, io.github.ppzxc.strand.core.internal.EndpointConfig.defaults(), false);
        closeables.add(peer);

        peer.sendBinary(new byte[8]);

        awaitWebSocketClose(peer);
        assertThat(status(peer)).isEqualTo(1002);
    }

    @Test
    void aWebSocketPingIsAnsweredButIsNotALivenessProbe() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);

        peer.sendPing(new byte[] {1, 2, 3});

        WsPeer.await(() -> peer.arrivals().contains("PONG"));
        assertThat(peer.pongs().get(0).payload).containsExactly(1, 2, 3);
        assertThat(peer.sawClosed()).isFalse();
    }

    @Test
    void webSocketPingsDoNotReplaceTheIdleStrandPing() throws Exception {
        startServer(
                credentials -> AuthenticationResult.accepted("peer"),
                connection -> PreparedHandler.of(request -> request.respond(Status.OK, request.payload())),
                Duration.ofMillis(300),
                null);
        WsPeer peer = peer();
        awaitOpen(peer);
        int framesBefore = peer.binaryMessages().size();

        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(900);
        while (System.nanoTime() < until) {
            peer.sendPing(new byte[0]);
            Thread.sleep(40);
        }

        assertThat(peer.binaryMessages().size()).isGreaterThan(framesBefore);
        assertThat(peer.sawClosed()).isFalse();
    }
}
