package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Status;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NettyWebSocketTest {
    private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

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

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private NettyStrandServer echoServer(AppFixture fixture, AtomicReference<PreparationFailure> failure) {
        NettyStrandServer.Builder builder = fixture.serverBuilder()
                .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                .handler(request -> request.respond(Status.OK, request.payload()));
        if (failure != null) {
            builder.onConnectionFailure(failure::set);
        }
        return builder.build();
    }

    private Listener listen(NettyStrandServer server, Transport transport) throws Exception {
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback(), transport));
        closeables.add(listener::close);
        return listener;
    }

    private static RequestResult requestWhenReady(AppFixture fixture, NettyStrandConnection connection, byte[] body)
            throws Exception {
        RequestResult result =
                fixture.result(connection.request(Request.of(TYPE, body)).result());
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (result.status() == Status.UNAVAILABLE
                && result.certainty() == Certainty.NOT_PROCESSED
                && System.nanoTime() < limit) {
            Thread.sleep(10);
            result = fixture.result(connection.request(Request.of(TYPE, body)).result());
        }
        return result;
    }

    private static String httpStatusLine(int port, String request) throws Exception {
        try (Socket socket = new Socket(InetAddress.getLoopbackAddress(), port)) {
            socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS));
            OutputStream out = socket.getOutputStream();
            out.write(request.getBytes(StandardCharsets.US_ASCII));
            out.flush();
            ByteArrayOutputStream response = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            InputStream in = socket.getInputStream();
            int count;
            try {
                while ((count = in.read(buffer)) >= 0) {
                    response.write(buffer, 0, count);
                }
            } catch (IOException ignored) {
            }
            String text = new String(response.toByteArray(), StandardCharsets.US_ASCII);
            return text.contains("\r\n") ? text.substring(0, text.indexOf("\r\n")) : text;
        }
    }

    private static String upgradeRequest(String path, String subprotocol) {
        return "GET " + path + " HTTP/1.1\r\n"
                + "Host: localhost\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                + "Sec-WebSocket-Version: 13\r\n"
                + (subprotocol == null ? "" : "Sec-WebSocket-Protocol: " + subprotocol + "\r\n")
                + "\r\n";
    }

    @Test
    void clientAndServerExchangeRequestsAndResponsesOverWebSocket() throws Exception {
        AppFixture fixture = fixture();
        Listener listener = listen(echoServer(fixture, null), Transport.webSocket("/strand"));
        NettyStrandClient client = fixture.clientBuilder().build();

        NettyStrandConnection connection =
                AppFixture.get(client.connect(AppFixture.loopback(listener), Transport.webSocket("/strand")));
        closeables.add(connection::abort);
        RequestResult result = requestWhenReady(fixture, connection, bytes("hello"));

        assertThat(result.status()).isEqualTo(Status.OK);
        assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(result.payload()).isEqualTo(bytes("hello"));
    }

    @Test
    void connectAttemptWithATransportConnects() throws Exception {
        AppFixture fixture = fixture();
        Listener listener = listen(echoServer(fixture, null), Transport.webSocket());
        NettyStrandClient client = fixture.clientBuilder().build();

        NettyConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener), Transport.webSocket());
        NettyStrandConnection connection = AppFixture.get(attempt.result());
        closeables.add(connection::abort);

        assertThat(requestWhenReady(fixture, connection, bytes("x")).status()).isEqualTo(Status.OK);
    }

    @Test
    void callsWithoutATransportStayTcp() throws Exception {
        AppFixture fixture = fixture();
        NettyStrandServer server = echoServer(fixture, null);
        Listener tcpListener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(tcpListener::close);
        NettyStrandClient client = fixture.clientBuilder().build();

        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(tcpListener)));
        closeables.add(connection::abort);

        assertThat(requestWhenReady(fixture, connection, bytes("tcp")).payload())
                .isEqualTo(bytes("tcp"));
    }

    @Test
    void serverRejectsAPathThatDoesNotMatchExactlyWith404() throws Exception {
        AppFixture fixture = fixture();
        Listener listener = listen(echoServer(fixture, null), Transport.webSocket("/strand"));
        int port = AppFixture.loopback(listener).getPort();

        assertThat(httpStatusLine(port, upgradeRequest("/other", "strand"))).contains(" 404");
        assertThat(httpStatusLine(port, upgradeRequest("/strand/", "strand"))).contains(" 404");
        assertThat(httpStatusLine(port, upgradeRequest("/", "strand"))).contains(" 404");
    }

    @Test
    void serverRejectsAnHttpRequestThatIsNotAnUpgradeWith404() throws Exception {
        AppFixture fixture = fixture();
        Listener listener = listen(echoServer(fixture, null), Transport.webSocket("/strand"));
        int port = AppFixture.loopback(listener).getPort();

        String plain = "GET /strand HTTP/1.1\r\nHost: localhost\r\n\r\n";

        assertThat(httpStatusLine(port, plain)).contains(" 404");
    }

    @Test
    void serverRejectsAnUpgradeWithoutTheStrandSubprotocolWith400() throws Exception {
        AppFixture fixture = fixture();
        Listener listener = listen(echoServer(fixture, null), Transport.webSocket("/strand"));
        int port = AppFixture.loopback(listener).getPort();

        assertThat(httpStatusLine(port, upgradeRequest("/strand", null))).contains(" 400");
        assertThat(httpStatusLine(port, upgradeRequest("/strand", "chat, superchat")))
                .contains(" 400");
    }

    @Test
    void serverAcceptsAnUpgradeThatOffersStrandAmongOtherSubprotocols() throws Exception {
        AppFixture fixture = fixture();
        Listener listener = listen(echoServer(fixture, null), Transport.webSocket("/strand"));
        int port = AppFixture.loopback(listener).getPort();

        assertThat(httpStatusLine(port, upgradeRequest("/strand", "chat, strand")))
                .contains(" 101");
    }

    @Test
    void aRejectedUpgradeIsReportedAsAPreparationFailureAtTheUpgradeStage() throws Exception {
        AppFixture fixture = fixture();
        AtomicReference<PreparationFailure> failure = new AtomicReference<>();
        Listener listener = listen(echoServer(fixture, failure), Transport.webSocket("/strand"));

        httpStatusLine(AppFixture.loopback(listener).getPort(), upgradeRequest("/other", "strand"));
        awaitNotNull(failure);

        assertThat(failure.get().stage()).isEqualTo(PreparationStage.UPGRADE);
        assertThat(failure.get().cause()).isInstanceOf(WebSocketUpgradeException.class);
        assertThat(((WebSocketUpgradeException) failure.get().cause()).httpStatus())
                .isEqualTo(404);
    }

    @Test
    void clientFailsPreparationWhenTheServerDoesNotSelectStrand() throws Exception {
        AppFixture fixture = fixture();
        try (ServerSocket fake = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            Thread server = new Thread(() -> answerUpgradeWithoutSubprotocol(fake), "fake-ws-server");
            server.setDaemon(true);
            server.start();
            NettyStrandClient client = fixture.clientBuilder().build();

            CompletableFuture<NettyStrandConnection> result = AppFixture.cf(client.connect(
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), fake.getLocalPort()),
                    Transport.webSocket("/strand")));

            ExecutionException failure = org.junit.jupiter.api.Assertions.assertThrows(
                    ExecutionException.class, () -> result.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS));
            assertThat(failure.getCause()).isInstanceOf(PreparationFailedException.class);
            PreparationFailure preparation = ((PreparationFailedException) failure.getCause()).failure();
            assertThat(preparation.stage()).isEqualTo(PreparationStage.UPGRADE);
        }
    }

    @Test
    void clientRequestsTheStrandSubprotocolPathAndHost() throws Exception {
        AppFixture fixture = fixture();
        AtomicReference<String> seen = new AtomicReference<>();
        try (ServerSocket fake = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            Thread server = new Thread(
                    () -> {
                        try (Socket socket = fake.accept()) {
                            seen.set(readHeaders(socket));
                        } catch (IOException ignored) {
                        }
                    },
                    "fake-ws-server");
            server.setDaemon(true);
            server.start();
            NettyStrandClient client = fixture.clientBuilder().build();
            InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), fake.getLocalPort());

            AppFixture.cf(client.connect(address, Transport.webSocket("/strand/v1")));
            awaitNotNull(seen);

            String request = seen.get();
            assertThat(request).startsWith("GET /strand/v1 HTTP/1.1\r\n");
            assertThat(request.toLowerCase(java.util.Locale.ROOT))
                    .contains("sec-websocket-protocol: strand\r\n")
                    .contains("host: " + address.getHostString().toLowerCase(java.util.Locale.ROOT) + ":"
                            + fake.getLocalPort());
        }
    }

    private static String readHeaders(Socket socket) throws IOException {
        socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS));
        StringBuilder text = new StringBuilder();
        InputStream in = socket.getInputStream();
        while (!text.toString().endsWith("\r\n\r\n")) {
            int next = in.read();
            if (next < 0) {
                break;
            }
            text.append((char) next);
        }
        return text.toString();
    }

    private static void answerUpgradeWithoutSubprotocol(ServerSocket fake) {
        try (Socket socket = fake.accept()) {
            String request = readHeaders(socket);
            String key = null;
            for (String line : request.split("\r\n", -1)) {
                if (line.toLowerCase(java.util.Locale.ROOT).startsWith("sec-websocket-key:")) {
                    key = line.substring(line.indexOf(':') + 1).trim();
                }
            }
            String accept = Base64.getEncoder()
                    .encodeToString(MessageDigest.getInstance("SHA-1")
                            .digest((key + GUID).getBytes(StandardCharsets.US_ASCII)));
            socket.getOutputStream()
                    .write(("HTTP/1.1 101 Switching Protocols\r\n"
                                    + "Upgrade: websocket\r\n"
                                    + "Connection: Upgrade\r\n"
                                    + "Sec-WebSocket-Accept: " + accept + "\r\n\r\n")
                            .getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
            socket.getInputStream().read();
        } catch (Exception ignored) {
        }
    }

    static <T> void awaitNotNull(AtomicReference<T> reference) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (reference.get() == null && System.nanoTime() < limit) {
            Thread.sleep(10);
        }
        assertThat(reference.get()).as("기다리던 값").isNotNull();
    }
}
