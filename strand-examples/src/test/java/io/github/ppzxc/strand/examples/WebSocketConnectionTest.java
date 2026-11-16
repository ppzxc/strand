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
        server = ConnectionDriver.server(EndpointConfig.defaults().webSocket(), serverPassage)
                .handler(handlers, request -> {
                    String text = new String(request.body(), StandardCharsets.UTF_8);
                    request.respond(0, ("re:" + text).getBytes(StandardCharsets.UTF_8));
                })
                .listener(closedInto(serverClosed))
                .start();
        client = ConnectionDriver.client(EndpointConfig.defaults().webSocket(), clientPassage)
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
        server = ConnectionDriver.server(EndpointConfig.defaults().webSocket(), serverPassage)
                .handler(handlers, request -> {})
                .start();
        client = ConnectionDriver.client(EndpointConfig.defaults().webSocket(), clientPassage)
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
