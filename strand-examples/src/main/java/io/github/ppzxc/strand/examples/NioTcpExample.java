package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.StrandVersion;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionClosed;
import io.github.ppzxc.strand.nio.internal.ConnectionDriver;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import io.github.ppzxc.strand.nio.internal.TcpPassage;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 논블로킹 방식으로 요청 하나를 보낸다. 결과를 기다리지 않고 {@code Future}에 이어 붙인 단계(결과 처리, 연결 종료, 종료 알림)가 차례로 돈다.
 * 연결 드라이버가 NIO {@link SocketChannel}을 논블로킹으로 다루므로 부분 쓰기와 임의 위치에서 잘린 읽기는 신경 쓰지 않는다. 사용법: {@code
 * NioTcpExample [host] [port]} (상대는 {@link ServerExample})
 */
public final class NioTcpExample {

    private NioTcpExample() {}

    public static void main(String[] args) throws Exception {
        System.out.println("strand core " + StrandVersion.get());
        run(ExampleSupport.arg(args, 0, "localhost"), Integer.parseInt(ExampleSupport.arg(args, 1, "9000")));
    }

    static RequestResult run(String host, int port) throws Exception {
        ExecutorService executor = ExampleSupport.newHandlerExecutor("strand-nio-example");
        CompletableFuture<ConnectionClosed> closed = new CompletableFuture<>();
        try (ConnectionDriver driver = ConnectionDriver.client(
                        ExampleSupport.withAuth(EndpointConfig.defaults()),
                        TcpPassage.of(SocketChannel.open(new InetSocketAddress(host, port))))
                .executor(executor)
                .listener(ExampleSupport.printing("클라이언트", closed))
                .start()) {
            CompletableFuture<RequestResult> done = driver.request(
                            ExampleSupport.DEMO_REQUEST_TYPE, "hello".getBytes(StandardCharsets.UTF_8))
                    .result()
                    .whenComplete((result, failure) -> {
                        if (result != null) {
                            ExampleSupport.handleResult(result);
                        }
                        driver.closeConnection();
                    });
            // PING도 기다리지 않는다. 결과가 오는 대로 위 단계와 나란히 돈다.
            driver.ping(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
            // 호출한 스레드는 여기서 끝을 기다릴 뿐이다. 위 단계는 결과가 오는 대로 돈다.
            closed.get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return done.get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }
}
