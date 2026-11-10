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
}
