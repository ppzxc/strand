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

/**
 * 제품 공개 API({@link NioStrandClient})로 TCP 서버에 연결해 요청 하나를 보낸다. 연결과 요청 결과, 종료 알림은 모두 보존된 결과나 {@code CompletionStage}로 오고, 이
 * 예제는 호출한 스레드가 {@code get}으로 기다린다. 결과를 기다리지 않고 이어 붙이는 방식은 {@link NioAppExample}이 보인다. 사용법: {@code
 * TcpClientExample [host] [port]} (상대는 {@link ServerExample})
 */
public final class TcpClientExample {

    private TcpClientExample() {}

    public static void main(String[] args) throws Exception {
        System.out.println("strand core " + StrandVersion.get());
        run(ExampleSupport.arg(args, 0, "localhost"), Integer.parseInt(ExampleSupport.arg(args, 1, "9000")));
    }

    static RequestResult run(String host, int port) throws Exception {
        // 앱이 소유하는 실행기: 알림과 결과 전달이 여기서 돌아 전송 스레드를 막지 않는다. 끝나면 앱이 종료한다.
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
            // 성공은 TCP 연결·HELLO·연결 인증(AUTH)까지 끝났다는 뜻이다(규격 3.3절).
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
            // close()는 CLOSE 절차를 시작한다. 종료 알림은 결과 보존과 통로 정리 뒤에 온다.
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
