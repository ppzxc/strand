package io.github.ppzxc.strand.load;

import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.Status;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 서버 프로세스 역할. 선택한 전송의 서버를 띄우고 {@code READY <포트>} 한 줄을 표준 출력에 낸 뒤, 표준 입력이 닫히면 정리하고 끝난다. 받은 요청은 같은 본문으로 그대로 응답한다. 두 전송에
 * 같은 한도·시간 설정을 준다.
 */
final class ServerRole {
    /** netty 서버가 쓰는 이벤트 루프 스레드 수. 독립 실행(자체 이벤트 루프)만 잰다. */
    static final int NETTY_EVENT_LOOPS = 2;

    private ServerRole() {}

    /** 전송별 서버. 두 전송의 공개 진입점이 서로 다른 타입이라 얇게 감싼다. */
    private interface Backend {
        /** 리스닝을 시작하고 실제 포트를 돌려준다. */
        int listen() throws Exception;

        void shutdown() throws Exception;
    }

    static int run(RunConfig config, PrintStream out) throws Exception {
        ExecutorService executor = LoadSupport.newExecutor("strand-load-server");
        Backend backend = "netty".equals(config.transport)
                ? new NettyBackend(config, executor)
                : new NioBackend(config, executor);
        try {
            int port = backend.listen();
            out.println("READY " + port);
            out.flush();
            awaitStdinClosed();
            backend.shutdown();
        } finally {
            executor.shutdownNow();
        }
        return 0;
    }

    /** 하네스가 표준 입력을 닫거나 죽으면 서버도 끝난다. 고아 서버가 남지 않게 한다. */
    private static void awaitStdinClosed() throws IOException {
        byte[] buffer = new byte[64];
        while (System.in.read(buffer) >= 0) {
            // 입력은 무시한다.
        }
    }

    private static final class NioBackend implements Backend {
        private final io.github.ppzxc.strand.nio.NioStrandServer server;
        private io.github.ppzxc.strand.nio.Listener listener;

        NioBackend(RunConfig config, ExecutorService executor) {
            io.github.ppzxc.strand.nio.NioSecurity security =
                    io.github.ppzxc.strand.nio.NioSecurity.externallySecured();
            MinimumProgressSpace space = io.github.ppzxc.strand.nio.NioMinimumProgress.of(
                    security, LoadSupport.maxFrameSize(config.bodyBytes), config.inflight, config.inflight);
            ResourceLimits limits = LoadSupport.limits(space, config.connections, config.inflight, config.bodyBytes);
            this.server = io.github.ppzxc.strand.nio.NioStrandServer.builder()
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
                            ? io.github.ppzxc.strand.nio.AuthenticationResult.accepted(LoadSupport.PRINCIPAL)
                            : io.github.ppzxc.strand.nio.AuthenticationResult.rejected())
                    .handlerFactory(connection -> io.github.ppzxc.strand.nio.PreparedHandler.of(
                            request -> request.respond(Status.OK, request.payload()), () -> {}))
                    .onConnectionFailure(failure -> System.err.println("준비 실패: " + failure))
                    .build();
        }

        @Override
        public int listen() throws Exception {
            listener = server.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                    .toCompletableFuture()
                    .get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            return ((InetSocketAddress) listener.localAddress()).getPort();
        }

        @Override
        public void shutdown() throws Exception {
            listener.close();
            server.shutdown().toCompletableFuture().get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static final class NettyBackend implements Backend {
        private final EventLoopGroup eventLoops = new NioEventLoopGroup(NETTY_EVENT_LOOPS);
        private final io.github.ppzxc.strand.netty.NettyStrandServer server;
        private io.github.ppzxc.strand.netty.Listener listener;

        NettyBackend(RunConfig config, ExecutorService executor) {
            io.github.ppzxc.strand.netty.NettySecurity security =
                    io.github.ppzxc.strand.netty.NettySecurity.externallySecured();
            MinimumProgressSpace space = io.github.ppzxc.strand.netty.NettyMinimumProgress.of(
                    security, LoadSupport.maxFrameSize(config.bodyBytes), config.inflight, config.inflight);
            ResourceLimits limits = LoadSupport.limits(space, config.connections, config.inflight, config.bodyBytes);
            this.server = io.github.ppzxc.strand.netty.NettyStrandServer.builder()
                    .eventLoopGroup(eventLoops)
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
                            ? io.github.ppzxc.strand.netty.AuthenticationResult.accepted(LoadSupport.PRINCIPAL)
                            : io.github.ppzxc.strand.netty.AuthenticationResult.rejected())
                    .handlerFactory(connection -> io.github.ppzxc.strand.netty.PreparedHandler.of(
                            request -> request.respond(Status.OK, request.payload()), () -> {}))
                    .onConnectionFailure(failure -> System.err.println("준비 실패: " + failure))
                    .build();
        }

        @Override
        public int listen() throws Exception {
            listener = server.listen(
                            new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                            io.github.ppzxc.strand.netty.Transport.tcp())
                    .get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            return ((InetSocketAddress) listener.localAddress()).getPort();
        }

        @Override
        public void shutdown() throws Exception {
            listener.close();
            server.shutdown().get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            eventLoops.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }
}
