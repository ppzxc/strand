package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.core.StrandVersion;
import io.github.ppzxc.strand.nio.NioMinimumProgress;
import io.github.ppzxc.strand.nio.NioSecurity;
import io.github.ppzxc.strand.nio.NioStrandClient;
import io.github.ppzxc.strand.nio.NioStrandConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class TcpClientExample {

    private TcpClientExample() {}

    public static void main(String[] args) throws Exception {
        System.out.println("strand core " + StrandVersion.get());
        run(ExampleSupport.arg(args, 0, "localhost"), Integer.parseInt(ExampleSupport.arg(args, 1, "9000")));
    }

    static RequestResult run(String host, int port) throws Exception {

        ExecutorService executor = AppExampleSupport.newAppExecutor("strand-tcp-client-example");
        NioSecurity security = NioSecurity.externallySecured();
        MinimumProgressSpace space = NioMinimumProgress.of(
                security,
                AppExampleSupport.MAX_FRAME_SIZE,
                AppExampleSupport.REQUESTS_PER_CONNECTION,
                AppExampleSupport.REQUESTS_PER_CONNECTION);
        NioStrandClient client = NioStrandClient.builder()
                .executor(executor)
                .resourceLimits(AppExampleSupport.limits(space))
                .preparationDeadline(AppExampleSupport.PREPARATION_DEADLINE)
                .shutdownGrace(AppExampleSupport.SHUTDOWN_GRACE)
                .shutdownDeadline(AppExampleSupport.SHUTDOWN_DEADLINE)
                .maxFrameSize(AppExampleSupport.MAX_FRAME_SIZE)
                .security(security)
                .credentials(ExampleSupport.credentials())
                .build();
        try {

            NioStrandConnection connection = client.connect(new InetSocketAddress(host, port))
                    .toCompletableFuture()
                    .get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            System.out.println("[클라이언트] AUTH 통과");
            RequestResult result = connection
                    .request(Request.of(ExampleSupport.DEMO_REQUEST_TYPE, "hello".getBytes(StandardCharsets.UTF_8)))
                    .result()
                    .toCompletionStage(executor)
                    .toCompletableFuture()
                    .get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            ExampleSupport.handleResult(result);

            connection.close();
            CloseNotice notice = connection
                    .closed()
                    .toCompletionStage(executor)
                    .toCompletableFuture()
                    .get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            System.out.println("[클라이언트] 연결 종료 " + notice.kind());
            return result;
        } finally {
            client.shutdown().toCompletableFuture().get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            executor.shutdownNow();
        }
    }
}
