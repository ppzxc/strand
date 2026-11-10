package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.StrandVersion;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionClosed;
import io.github.ppzxc.strand.nio.internal.ConnectionDriver;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * {@link WebSocketPassage}(JDK 11 {@code java.net.http} WebSocket) 위에서 요청 하나를 보낸다. WebSocket은 메시지 하나가 프레임 하나이므로
 * {@link EndpointConfig#webSocket()}으로 설정한다. 나머지는 TCP 예제와 같다. 사용법: {@code WebSocketExample ws://host:port/path}
 * (strand를 WebSocket으로 받는 서버가 필요하다)
 */
public final class WebSocketExample {

    private WebSocketExample() {}

    public static void main(String[] args) throws Exception {
        System.out.println("strand core " + StrandVersion.get());
        run(URI.create(ExampleSupport.arg(args, 0, "ws://localhost:8080/")));
    }

    static RequestResult run(URI uri) throws Exception {
        ExecutorService executor = ExampleSupport.newHandlerExecutor("strand-websocket-example");
        CompletableFuture<ConnectionClosed> closed = new CompletableFuture<>();
        try (ConnectionDriver driver = ConnectionDriver.client(
                        ExampleSupport.withAuth(EndpointConfig.defaults().webSocket()), WebSocketPassage.connect(uri))
                .executor(executor)
                .listener(ExampleSupport.printing("클라이언트", closed))
                .start()) {
            RequestResult result = driver.request(
                            ExampleSupport.DEMO_REQUEST_TYPE, "hello".getBytes(StandardCharsets.UTF_8))
                    .result()
                    .get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            ExampleSupport.handleResult(result);
            driver.closeConnection();
            closed.get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return result;
        } finally {
            executor.shutdownNow();
        }
    }
}
