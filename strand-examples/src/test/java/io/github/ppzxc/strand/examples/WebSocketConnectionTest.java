package io.github.ppzxc.strand.examples;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionClosed;
import io.github.ppzxc.strand.nio.internal.ConnectionDriver;
import io.github.ppzxc.strand.nio.internal.ConnectionListener;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** WebSocket 통로 위에서 두 연결 드라이버가 요청을 주고받고 WebSocket Close로 끝나는지. 소켓은 {@link FakeWebSocket}으로 잇는다. */
class WebSocketConnectionTest {

    private final ExecutorService handlers = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "test-handler");
        thread.setDaemon(true);
        return thread;
    });
    private ConnectionDriver client;
    private ConnectionDriver server;

    @AfterEach
    void stop() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
        handlers.shutdownNow();
    }

    private static <T> T within(CompletableFuture<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
    }

    private static ConnectionListener closedInto(CompletableFuture<ConnectionClosed> target) {
        return new ConnectionListener() {
            @Override
            public void onClosed(ConnectionClosed event) {
                target.complete(event);
            }
        };
    }

    @Test
    void aRequestGetsItsResponseAndTheConnectionEndsWithAWebSocketClose() throws Exception {
        WebSocketPassage clientPassage = new WebSocketPassage();
        WebSocketPassage serverPassage = new WebSocketPassage();
        FakeWebSocket[] sockets = FakeWebSocket.link(clientPassage, serverPassage);
        CompletableFuture<ConnectionClosed> clientClosed = new CompletableFuture<>();
        CompletableFuture<ConnectionClosed> serverClosed = new CompletableFuture<>();
        server = ConnectionDriver.server(EndpointConfig.defaults(), serverPassage)
                .handler(handlers, request -> {
                    String text = new String(request.body(), StandardCharsets.UTF_8);
                    request.respond(0, ("re:" + text).getBytes(StandardCharsets.UTF_8));
                })
                .listener(closedInto(serverClosed))
                .start();
        client = ConnectionDriver.client(EndpointConfig.defaults(), clientPassage)
                .executor(handlers)
                .listener(closedInto(clientClosed))
                .start();

        RequestResult result = within(
                client.request(256, "hi".getBytes(StandardCharsets.UTF_8)).result());
        client.closeConnection();

        assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(new String(result.body(), StandardCharsets.UTF_8)).isEqualTo("re:hi");
        assertThat(within(clientClosed).reason().kind()).isEqualTo(CloseReason.Kind.CLOSE);
        assertThat(within(serverClosed).reason().kind()).isEqualTo(CloseReason.Kind.CLOSE);
        // 종료 알림은 전송 스레드가 WebSocket Close를 보내기 전에 나갈 수 있어 보내질 때까지 기다린다.
        awaitUntil(() -> sockets[0].sentCloseCodes.contains(1000));
        assertThat(sockets[0].sentCloseCodes).as("클라이언트가 보낸 WebSocket Close").contains(1000);
    }

    /**
     * 앞선 보내기가 끝나지 않아 이어 붙은 Close가 나중에 실패해도(write는 0을 돌려준다) 정상 CLOSE 교환이 입출력 실패로 보이지 않는다. 실패는
     * Close 교환이 끝나지 않은 연결에서만 입출력 실패 원인이 된다.
     */
    @Test
    void aChainedCloseThatFailsLaterNeverMixesAnIoFailureIntoACloseExchange() throws Exception {
        WebSocketPassage clientPassage = new WebSocketPassage();
        WebSocketPassage serverPassage = new WebSocketPassage();
        FakeWebSocket[] sockets = FakeWebSocket.link(clientPassage, serverPassage);
        CompletableFuture<ConnectionClosed> clientClosed = new CompletableFuture<>();
        server = ConnectionDriver.server(EndpointConfig.defaults(), serverPassage)
                .handler(handlers, request -> {})
                .start();
        client = ConnectionDriver.client(EndpointConfig.defaults(), clientPassage)
                .executor(handlers)
                .listener(closedInto(clientClosed))
                .start();
        awaitUntil(() -> client.writtenFrames() >= 1);
        sockets[0].holdSends = true;
        long before = client.writtenFrames();
        client.ping(new byte[8]);
        awaitUntil(() -> client.writtenFrames() > before);

        client.closeConnection();
        sockets[0].failSends = true;
        sockets[0].releaseSends();

        ConnectionClosed closed = within(clientClosed);
        // 실패가 CLOSE 교환보다 먼저 닿으면 입출력 실패로 끊긴 것이고, 교환이 먼저 끝났으면 입출력 실패는 원인으로 남지 않는다. 어느 쪽이든 둘이 섞이지 않는다.
        boolean closeExchange = closed.reason().kind() == CloseReason.Kind.CLOSE;
        assertThat(closed.ioException().isPresent()).isEqualTo(!closeExchange);
    }

    /** 텍스트 메시지는 바이트로 넘길 수 없는 입력이다. 서문 단계라 core가 아무것도 보내지 않고 닫는다. */
    @Test
    void aTextMessageBeforeThePrefaceClosesTheConnectionWithoutAnyStrandBytes() throws Exception {
        WebSocketPassage serverPassage = new WebSocketPassage();
        FakeWebSocket socket = new FakeWebSocket();
        serverPassage.listener().onOpen(socket);
        CompletableFuture<ConnectionClosed> serverClosed = new CompletableFuture<>();
        server = ConnectionDriver.server(EndpointConfig.defaults(), serverPassage)
                .handler(handlers, request -> {})
                .listener(closedInto(serverClosed))
                .start();

        serverPassage.listener().onText(socket, "hello", true);

        assertThat(within(serverClosed).reason().kind()).isNotEqualTo(CloseReason.Kind.CLOSE);
        assertThat(socket.sentBinary).as("서문 단계의 위반에는 아무것도 보내지 않는다").isEmpty();
    }

    /** 오류로 끝난 연결은 WebSocket Close 1000으로 닫지 않는다(규격 1.2.1절). */
    @Test
    void aConnectionThatEndsWithAnErrorCloseIsNotClosedWithNormalClosure() throws Exception {
        WebSocketPassage clientPassage = new WebSocketPassage();
        WebSocketPassage serverPassage = new WebSocketPassage();
        FakeWebSocket[] sockets = FakeWebSocket.link(clientPassage, serverPassage);
        CompletableFuture<ConnectionClosed> serverClosed = new CompletableFuture<>();
        server = ConnectionDriver.server(EndpointConfig.defaults(), serverPassage)
                .handler(handlers, request -> {})
                .listener(closedInto(serverClosed))
                .start();
        client = ConnectionDriver.client(EndpointConfig.defaults(), clientPassage)
                .executor(handlers)
                .start();
        awaitUntil(() -> client.writtenFrames() >= 1);

        // 길이 필드가 최소보다 작은 프레임은 연결 오류다.
        sockets[0].sendBinary(java.nio.ByteBuffer.wrap(new byte[] {0, 0, 0, 1, 0}), true);

        assertThat(within(serverClosed).reason().kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
        awaitUntil(() -> !sockets[1].sentCloseCodes.isEmpty());
        assertThat(sockets[1].sentCloseCodes).doesNotContain(1000);
    }

    private static void awaitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("5초 안에 조건이 이뤄지지 않았다");
            }
            Thread.sleep(5);
        }
    }
}
