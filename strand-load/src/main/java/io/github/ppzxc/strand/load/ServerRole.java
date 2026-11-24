package io.github.ppzxc.strand.load;

import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.nio.AuthenticationResult;
import io.github.ppzxc.strand.nio.Listener;
import io.github.ppzxc.strand.nio.NioMinimumProgress;
import io.github.ppzxc.strand.nio.NioSecurity;
import io.github.ppzxc.strand.nio.NioStrandServer;
import io.github.ppzxc.strand.nio.PreparedHandler;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 서버 프로세스 역할. 선택한 전송의 서버를 띄우고 {@code READY <포트>} 한 줄을 표준 출력에 낸 뒤, 표준 입력이 닫히면 정리하고 끝난다. 받은 요청은 같은 본문으로 그대로 응답한다.
 */
final class ServerRole {
    private ServerRole() {}

    static int run(RunConfig config, PrintStream out) throws Exception {
        ExecutorService executor = LoadSupport.newExecutor("strand-load-server");
        NioSecurity security = NioSecurity.externallySecured();
        MinimumProgressSpace space = NioMinimumProgress.of(
                security, LoadSupport.maxFrameSize(config.bodyBytes), config.inflight, config.inflight);
        ResourceLimits limits = LoadSupport.limits(space, config.connections, config.inflight, config.bodyBytes);
        NioStrandServer server = NioStrandServer.builder()
                .executor(executor)
                .resourceLimits(limits)
                .preparationDeadline(LoadSupport.PREPARATION_DEADLINE)
                .shutdownGrace(LoadSupport.SHUTDOWN_GRACE)
                .shutdownDeadline(LoadSupport.SHUTDOWN_DEADLINE)
                .writeQueueBytes(LoadSupport.WRITE_QUEUE_BYTES)
                .closeFinishDeadline(LoadSupport.CLOSE_FINISH_DEADLINE)
                .maxFrameSize(LoadSupport.maxFrameSize(config.bodyBytes))
                .security(security)
                .authenticate(credentials -> LoadSupport.AUTH_MECHANISM.equals(credentials.mechanism())
                                && Arrays.equals(LoadSupport.AUTH_SECRET, credentials.secret())
                        ? AuthenticationResult.accepted(LoadSupport.PRINCIPAL)
                        : AuthenticationResult.rejected())
                .handlerFactory(connection ->
                        PreparedHandler.of(request -> request.respond(Status.OK, request.payload()), () -> {}))
                .onConnectionFailure(failure -> System.err.println("준비 실패: " + failure))
                .build();
        Listener listener = server.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                .toCompletableFuture()
                .get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
        int port = ((InetSocketAddress) listener.localAddress()).getPort();
        out.println("READY " + port);
        out.flush();
        awaitStdinClosed();
        listener.close();
        server.shutdown().toCompletableFuture().get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
        executor.shutdownNow();
        return 0;
    }

    /** 하네스가 표준 입력을 닫거나 죽으면 서버도 끝난다. 고아 서버가 남지 않게 한다. */
    private static void awaitStdinClosed() throws IOException {
        byte[] buffer = new byte[64];
        while (System.in.read(buffer) >= 0) {
            // 입력은 무시한다.
        }
    }
}
