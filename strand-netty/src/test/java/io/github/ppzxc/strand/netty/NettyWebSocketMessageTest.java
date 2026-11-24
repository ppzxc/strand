package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.ErrorCode;
import io.github.ppzxc.strand.core.internal.Event;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * WebSocket 서버가 메시지 통로의 입력 규칙(규격 1.2절·3.1절·5.1절)대로 반응하는지 실제 소켓과 직접 만든 프레임으로 확인한다: 텍스트 메시지, 크기가 다른 메시지, 조각 모으기, 한도 초과,
 * 서문 위반, WebSocket 끊김.
 */
class NettyWebSocketMessageTest {
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

    private void startServer() throws Exception {
        startServer(AppFixture.plain());
    }

    private void startServer(AppFixture appFixture) throws Exception {
        fixture = appFixture;
        closeables.add(fixture);
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> AuthenticationResult.accepted("peer"))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> request.respond(Status.OK, request.payload()));
                })
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback(), Transport.webSocket(PATH)));
        closeables.add(listener::close);
        port = AppFixture.loopback(listener).getPort();
    }

    private WsPeer peer() throws Exception {
        WsPeer peer = WsPeer.connect(port, PATH);
        closeables.add(peer);
        return peer;
    }

    /** 연결이 열림 단계에 이를 때까지(서버의 AUTH 확인을 받을 때까지) 기다린다. */
    private static void awaitOpen(WsPeer peer) throws Exception {
        WsPeer.await(() -> peer.events().stream().anyMatch(event -> event.kind() == Event.Kind.AUTHENTICATED));
    }

    private static CloseReason closedReason(WsPeer peer) {
        return peer.events().stream()
                .filter(event -> event.kind() == Event.Kind.CLOSED)
                .findFirst()
                .get()
                .closeReason()
                .get();
    }

    @Test
    void aTextMessageIsAConnectionError() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);

        peer.sendText("hello");

        WsPeer.await(peer::sawClosed);
        CloseReason reason = closedReason(peer);
        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
        assertThat(reason.errorCode()).isEqualTo(ErrorCode.PROTOCOL_ERROR);
        assertThat(reason.sender().get()).isEqualTo(CloseReason.Sender.PEER);
    }

    @Test
    void aMessageHoldingTwoFramesIsAConnectionError() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);
        byte[] ping = peer.runDetached(endpoint -> endpoint.ping(new byte[8])).get(0);
        ByteArrayOutputStream twoFrames = new ByteArrayOutputStream();
        twoFrames.write(ping);
        twoFrames.write(ping);

        peer.sendBinary(twoFrames.toByteArray());

        WsPeer.await(peer::sawClosed);
        assertThat(closedReason(peer).errorCode()).isEqualTo(ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    void aMessageHoldingHalfAFrameIsAConnectionError() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);
        byte[] ping = peer.runDetached(endpoint -> endpoint.ping(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}))
                .get(0);

        peer.sendBinary(java.util.Arrays.copyOf(ping, ping.length - 1));

        WsPeer.await(peer::sawClosed);
        assertThat(closedReason(peer).errorCode()).isEqualTo(ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    void aFragmentedMessageIsAssembledIntoOneFrame() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);
        byte[] body = new byte[3000];
        java.util.Arrays.fill(body, (byte) 9);
        byte[] request = peer.runDetached(endpoint -> endpoint.request(AppFixture.TYPE, body))
                .get(0);

        peer.sendFragmented(request, 5);

        WsPeer.await(() -> peer.events().stream().anyMatch(event -> event.kind() == Event.Kind.RESULT));
        Event result = peer.events().stream()
                .filter(event -> event.kind() == Event.Kind.RESULT)
                .findFirst()
                .get();
        assertThat(result.status()).isEqualTo(Status.OK);
        assertThat(result.body()).isEqualTo(body);
        assertThat(peer.sawClosed()).isFalse();
    }

    @Test
    void aFrameLongerThanMaxFrameSizeIsRejectedBeforeItsPayloadArrives() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);

        // 한도보다 긴 길이를 주장하는 머리만 보낸다. 페이로드가 오기를 기다리지 않고 거절해야 한다(메모리를 쓰기 전).
        peer.sendRaw(RawWebSocket.headerClaiming(RawWebSocket.BINARY, true, fixture.maxFrameSize + 1, true));

        WsPeer.await(peer::sawClosed);
        assertThat(closedReason(peer).errorCode()).isEqualTo(ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    void fragmentsThatAddUpPastMaxFrameSizeAreRejectedBeforeTheLastFragment() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);
        int piece = (int) (fixture.maxFrameSize / 2) + 1;

        peer.sendRaw(RawWebSocket.encode(RawWebSocket.BINARY, false, new byte[piece], true));
        peer.sendRaw(RawWebSocket.encode(RawWebSocket.CONTINUATION, false, new byte[piece], true));
        // 마지막 조각(fin)은 보내지 않았다. 누적이 한도를 넘는 순간 닫혀야 한다.

        WsPeer.await(peer::sawClosed);
        assertThat(closedReason(peer).errorCode()).isEqualTo(ErrorCode.PROTOCOL_ERROR);
    }

    @Test
    void anInvalidPrefaceClosesWithoutSendingAConnectionClose() throws Exception {
        startServer();
        WsPeer peer = WsPeer.connect(port, PATH, io.github.ppzxc.strand.core.internal.EndpointConfig.defaults(), false);
        closeables.add(peer);

        peer.sendBinary(new byte[8]);

        WsPeer.await(() -> !peer.closes().isEmpty());
        // 서문 오류는 아무것도 보내지 않고 닫는다: 서버의 Strand CLOSE 프레임이 없다.
        assertThat(peer.sawClosed()).isFalse();
        assertThat(peer.binaryMessages()).noneMatch(message -> message.length > 8);
    }

    @Test
    void aPeerThatClosesWebSocketWithoutAConnectionCloseIsReportedAsDisconnected() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);
        NettyStrandConnection connection = accepted.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);

        peer.sendClose(1001);

        CloseNotice notice = fixture.result(connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        // 상대가 먼저 닫으면 상대의 코드를 그대로 돌려준다(앱에는 노출하지 않는다).
        WsPeer.await(() -> !peer.closes().isEmpty());
        assertThat(peer.closes().get(0).closeStatus()).isEqualTo(1001);
    }

    @Test
    void aSocketThatDropsIsReportedAsDisconnected() throws Exception {
        startServer();
        WsPeer peer = peer();
        awaitOpen(peer);
        NettyStrandConnection connection = accepted.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);

        peer.disconnect();

        assertThat(fixture.result(connection.closed()).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
    }

    @Test
    void theClientSideTreatsATextMessageFromTheServerAsAConnectionError() throws Exception {
        AppFixture appFixture = AppFixture.plain();
        closeables.add(appFixture);
        java.net.ServerSocket fake = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress());
        closeables.add(fake);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread server = new Thread(
                () -> {
                    try (java.net.Socket socket = fake.accept()) {
                        String request = RawWebSocket.readHeaders(socket.getInputStream());
                        String key = RawWebSocket.header(request, "sec-websocket-key");
                        socket.getOutputStream()
                                .write(
                                        ("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                                                        + "Sec-WebSocket-Accept: " + RawWebSocket.acceptFor(key)
                                                        + "\r\n"
                                                        + "Sec-WebSocket-Protocol: strand\r\n\r\n")
                                                .getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                        socket.getOutputStream().flush();
                        // 서문 대신 텍스트 메시지를 보낸다.
                        RawWebSocket.write(
                                socket.getOutputStream(),
                                RawWebSocket.encode(RawWebSocket.TEXT, true, new byte[] {'h', 'i'}, false));
                        while (socket.getInputStream().read() >= 0) {
                            // 클라이언트가 닫을 때까지 읽는다.
                        }
                    } catch (Exception e) {
                        failure.set(e);
                    }
                },
                "fake-ws-server");
        server.setDaemon(true);
        server.start();

        java.util.concurrent.ExecutionException result = org.junit.jupiter.api.Assertions.assertThrows(
                java.util.concurrent.ExecutionException.class,
                () -> AppFixture.get(appFixture
                        .clientBuilder()
                        .build()
                        .connect(
                                new java.net.InetSocketAddress(
                                        java.net.InetAddress.getLoopbackAddress(), fake.getLocalPort()),
                                Transport.webSocket())));
        assertThat(result.getCause()).isInstanceOf(PreparationFailedException.class);
        assertThat(((PreparationFailedException) result.getCause()).failure().stage())
                .isEqualTo(PreparationStage.HELLO);
    }
}
