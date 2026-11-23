package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.nio.AuthenticationResult;
import io.github.ppzxc.strand.nio.Listener;
import io.github.ppzxc.strand.nio.NioMinimumProgress;
import io.github.ppzxc.strand.nio.NioSecurity;
import io.github.ppzxc.strand.nio.NioStrandServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 제품 공개 API({@link NioStrandServer})로 TCP 서버를 연다. 연결 인증은 예제의 토큰만 통과시키고, 받은 요청에는 핸들러가 {@code echo:} 뒤에 본문을 붙여 {@code OK}로
 * 응답한다. 클라이언트 예제({@link TcpClientExample})의 상대로 쓴다. 사용법: {@code ServerExample [port]}
 */
public final class ServerExample {

    private ServerExample() {}

    public static void main(String[] args) throws Exception {
        Server server = start(Integer.parseInt(ExampleSupport.arg(args, 0, "9000")));
        System.out.println("대기 중 " + server.port());
        Thread.currentThread().join();
    }

    /**
     * 서버를 열고 연결을 받기 시작한다.
     *
     * @param port 받을 포트. 0이면 운영체제가 빈 포트를 고른다
     */
    static Server start(int port) throws Exception {
        // 앱이 소유하는 실행기: 연결 인증과 핸들러가 여기서 돌아 전송 스레드를 막지 않는다. 서버를 닫을 때 앱이 종료한다.
        ExecutorService executor = AppExampleSupport.newAppExecutor("strand-server-example");
        NioSecurity security = NioSecurity.externallySecured();
        MinimumProgressSpace space = NioMinimumProgress.of(
                security,
                AppExampleSupport.MAX_FRAME_SIZE,
                AppExampleSupport.REQUESTS_PER_CONNECTION,
                AppExampleSupport.REQUESTS_PER_CONNECTION);
        NioStrandServer server = NioStrandServer.builder()
                .executor(executor)
                .resourceLimits(AppExampleSupport.limits(space))
                .preparationDeadline(AppExampleSupport.PREPARATION_DEADLINE)
                .shutdownGrace(AppExampleSupport.SHUTDOWN_GRACE)
                .shutdownDeadline(AppExampleSupport.SHUTDOWN_DEADLINE)
                .maxFrameSize(AppExampleSupport.MAX_FRAME_SIZE)
                .security(security)
                .authenticate(credentials -> ExampleSupport.AUTH_MECHANISM.equals(credentials.mechanism())
                                && Arrays.equals(ExampleSupport.AUTH_CREDENTIALS, credentials.secret())
                        ? AuthenticationResult.accepted(AppExampleSupport.PRINCIPAL)
                        : AuthenticationResult.rejected())
                .handler(request -> {
                    String body = new String(request.payload(), StandardCharsets.UTF_8);
                    System.out.println("[서버] 요청 type=" + request.type() + " body=" + body);
                    request.respond(Status.OK, ("echo:" + body).getBytes(StandardCharsets.UTF_8));
                })
                .build();
        try {
            Listener listener = server.listen(new InetSocketAddress(port))
                    .toCompletableFuture()
                    .get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return new Server(server, listener, executor);
        } catch (Exception | Error e) {
            executor.shutdownNow();
            throw e;
        }
    }

    /** 열려 있는 서버. 닫으면 받는 것을 멈추고 이어진 연결을 모두 끊는다. */
    static final class Server implements AutoCloseable {
        private final NioStrandServer server;
        private final Listener listener;
        private final ExecutorService executor;

        private Server(NioStrandServer server, Listener listener, ExecutorService executor) {
            this.server = server;
            this.listener = listener;
            this.executor = executor;
        }

        int port() {
            return ((InetSocketAddress) listener.localAddress()).getPort();
        }

        @Override
        public void close() {
            listener.close();
            try {
                server.shutdown().toCompletableFuture().get(ExampleSupport.TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // 닫는 중의 오류는 할 수 있는 일이 없다. 연결은 종료 기한이 지나면 끊긴다.
            } finally {
                executor.shutdownNow();
            }
        }
    }
}
