package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.StrandVersion;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionClosed;
import io.github.ppzxc.strand.nio.internal.ConnectionDriver;
import io.github.ppzxc.strand.nio.internal.RequestHandle;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import io.github.ppzxc.strand.nio.internal.TcpPassage;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 블로킹 방식으로 요청 하나를 보낸다. 요청은 {@code Future}를 돌려주고, 호출한 스레드는 {@code get}으로 결과를 기다린다. 끝낼 때는
 * {@code closeConnection}으로 CLOSE를 보내고 종료 알림을 기다린다. 사용법: {@code BlockingTcpExample [host] [port]} (상대는
 * {@link ServerExample})
 */
public final class BlockingTcpExample {

    private BlockingTcpExample() {}

    public static void main(String[] args) throws Exception {
        System.out.println("strand core " + StrandVersion.get());
        run(ExampleSupport.arg(args, 0, "localhost"), Integer.parseInt(ExampleSupport.arg(args, 1, "9000")));
    }

    static RequestResult run(String host, int port) throws Exception {
        ExecutorService executor = ExampleSupport.newHandlerExecutor("strand-blocking-example");
        CompletableFuture<ConnectionClosed> closed = new CompletableFuture<>();
        try (ConnectionDriver driver = ConnectionDriver.client(
                        ExampleSupport.withAuth(EndpointConfig.defaults()),
                        TcpPassage.of(SocketChannel.open(new InetSocketAddress(host, port))))
                .executor(executor)
                .listener(ExampleSupport.printing("클라이언트", closed))
                .start()) {
            // AUTH 관문을 지나기 전의 요청은 core가 미뤘다가 AUTH(ACK) 뒤에 보낸다(규격 3.3절).
            RequestHandle handle =
                    driver.request(ExampleSupport.DEMO_REQUEST_TYPE, "hello".getBytes(StandardCharsets.UTF_8));
            // PING은 연결이 살아 있는지 확인한다. 보냈다는 것(명령 처리)까지만 기다린다.
            driver.ping(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}).get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            RequestResult result = handle.result().get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            ExampleSupport.handleResult(result);
            driver.closeConnection();
            closed.get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return result;
        } finally {
            executor.shutdownNow();
        }
    }
}
