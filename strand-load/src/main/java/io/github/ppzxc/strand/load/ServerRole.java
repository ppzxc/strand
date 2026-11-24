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

    private static final java.util.concurrent.ScheduledExecutorService DELAYS =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "strand-load-server-delay");
                thread.setDaemon(true);
                return thread;
            });

    private ServerRole() {}

    /** 상대가 읽지 않는 연결로 요청을 계속 밀어 넣는다. 하나가 끝나면 다음을 보내 미완료 수를 유지한다. 연결이 끝나면 결과가 실패로 돌아와 멈춘다. */
    private static void push(io.github.ppzxc.strand.nio.NioStrandConnection connection, int bodyBytes) {
        connection
                .request(io.github.ppzxc.strand.api.Request.of(LoadSupport.ECHO_TYPE, new byte[bodyBytes]))
                .result()
                .observe(result -> {
                    if (result.status() == Status.OK) {
                        push(connection, bodyBytes);
                    }
                });
    }

    private static void push(io.github.ppzxc.strand.netty.NettyStrandConnection connection, int bodyBytes) {
        connection
                .request(io.github.ppzxc.strand.api.Request.of(LoadSupport.ECHO_TYPE, new byte[bodyBytes]))
                .result()
                .observe(result -> {
                    if (result.status() == Status.OK) {
                        push(connection, bodyBytes);
                    }
                });
    }

    /** 서버 쪽 연결 종료를 {@code CLOSED <json>} 한 줄로 알린다. */
    private static void announceClosed(PrintStream out, io.github.ppzxc.strand.api.CloseNotice notice) {
        com.google.gson.JsonObject json = new com.google.gson.JsonObject();
        json.addProperty("kind", notice.kind().name());
        json.addProperty("errorCode", notice.errorCode());
        json.addProperty("sender", notice.sender().map(Enum::name).orElse("none"));
        json.addProperty(
                "cause",
                notice.cause()
                        .map(c -> c.getClass().getName() + ": " + c.getMessage())
                        .orElse("none"));
        out.println("CLOSED " + json);
        out.flush();
    }

    /** 하네스 자체 시험용 지연. 0이면 바로 응답한다. */
    private static void respondAfterDelay(RunConfig config, Runnable respond) {
        if (config.serverDelay.isZero()) {
            respond.run();
        } else {
            DELAYS.schedule(respond, config.serverDelay.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    /** 전송별 서버. 두 전송의 공개 진입점이 서로 다른 타입이라 얇게 감싼다. */
    private interface Backend {
        /** 리스닝을 시작하고 실제 포트를 돌려준다. */
        int listen() throws Exception;

        void shutdown() throws Exception;
    }

    static int run(RunConfig config, PrintStream out) throws Exception {
        ExecutorService executor = LoadSupport.newExecutor("strand-load-server");
        Backend backend = "netty".equals(config.transport)
                ? new NettyBackend(config, executor, out)
                : new NioBackend(config, executor, out);
        try {
            int port = backend.listen();
            out.println("READY " + port);
            out.flush();
            serveControl(out);
            backend.shutdown();
        } finally {
            executor.shutdownNow();
        }
        return 0;
    }

    /**
     * 하네스가 표준 입력을 닫거나 죽으면 서버도 끝난다. 고아 서버가 남지 않게 한다. 입력 줄 {@code SAMPLE}에는 자원 표본 한 줄({@code SAMPLE <json>})로 답한다.
     */
    private static void serveControl(PrintStream out) throws IOException {
        java.io.BufferedReader reader = new java.io.BufferedReader(
                new java.io.InputStreamReader(System.in, java.nio.charset.StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.equals("SAMPLE")) {
                out.println("SAMPLE " + ResourceSampler.sample());
                out.flush();
            }
        }
    }

    private static final class NioBackend implements Backend {
        private final io.github.ppzxc.strand.nio.NioStrandServer server;
        private io.github.ppzxc.strand.nio.Listener listener;

        NioBackend(RunConfig config, ExecutorService executor, PrintStream out) throws Exception {
            io.github.ppzxc.strand.nio.NioSecurity security = config.tls
                    ? io.github.ppzxc.strand.nio.NioSecurity.tls(
                            LoadTls.serverContext(java.nio.file.Paths.get(config.keystore)))
                    : io.github.ppzxc.strand.nio.NioSecurity.externallySecured();
            MinimumProgressSpace space = io.github.ppzxc.strand.nio.NioMinimumProgress.of(
                    security,
                    LoadSupport.maxFrameSize(config.bodyBytes),
                    config.effectiveInflight(),
                    config.effectiveInflight());
            ResourceLimits limits =
                    LoadSupport.limits(space, config.connections, config.effectiveInflight(), config.bodyBytes);
            this.server = io.github.ppzxc.strand.nio.NioStrandServer.builder()
                    .executor(executor)
                    .resourceLimits(limits)
                    .preparationDeadline(LoadSupport.PREPARATION_DEADLINE)
                    .shutdownGrace(LoadSupport.SHUTDOWN_GRACE)
                    .shutdownDeadline(LoadSupport.SHUTDOWN_DEADLINE)
                    .idlePingInterval(config.idlePing)
                    .ackTimeout(config.ackTimeout)
                    .writeQueueBytes(LoadSupport.WRITE_QUEUE_BYTES)
                    .closeFinishDeadline(LoadSupport.CLOSE_FINISH_DEADLINE)
                    .maxFrameSize(LoadSupport.maxFrameSize(config.bodyBytes))
                    .security(security)
                    .authenticate(credentials -> LoadSupport.AUTH_MECHANISM.equals(credentials.mechanism())
                                    && Arrays.equals(LoadSupport.AUTH_SECRET, credentials.secret())
                            ? io.github.ppzxc.strand.nio.AuthenticationResult.accepted(LoadSupport.PRINCIPAL)
                            : io.github.ppzxc.strand.nio.AuthenticationResult.rejected())
                    .handlerFactory(connection -> {
                        if (config.stall) {
                            connection.closed().observe(notice -> announceClosed(out, notice));
                        }
                        return io.github.ppzxc.strand.nio.PreparedHandler.of(
                                request -> {
                                    if (config.stall && request.type() == LoadSupport.STALL_TYPE) {
                                        request.respond(Status.OK, new byte[0]);
                                        for (int i = 0; i < config.inflight; i++) {
                                            push(request.connection(), config.bodyBytes);
                                        }
                                        return;
                                    }
                                    respondAfterDelay(config, () -> request.respond(Status.OK, request.payload()));
                                },
                                () -> {});
                    })
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

        NettyBackend(RunConfig config, ExecutorService executor, PrintStream out) throws Exception {
            io.github.ppzxc.strand.netty.NettySecurity security = config.tls
                    ? io.github.ppzxc.strand.netty.NettySecurity.tls(
                            LoadTls.serverContext(java.nio.file.Paths.get(config.keystore)))
                    : io.github.ppzxc.strand.netty.NettySecurity.externallySecured();
            MinimumProgressSpace space = io.github.ppzxc.strand.netty.NettyMinimumProgress.of(
                    security,
                    LoadSupport.maxFrameSize(config.bodyBytes),
                    config.effectiveInflight(),
                    config.effectiveInflight());
            ResourceLimits limits =
                    LoadSupport.limits(space, config.connections, config.effectiveInflight(), config.bodyBytes);
            this.server = io.github.ppzxc.strand.netty.NettyStrandServer.builder()
                    .eventLoopGroup(eventLoops)
                    .executor(executor)
                    .resourceLimits(limits)
                    .preparationDeadline(LoadSupport.PREPARATION_DEADLINE)
                    .shutdownGrace(LoadSupport.SHUTDOWN_GRACE)
                    .shutdownDeadline(LoadSupport.SHUTDOWN_DEADLINE)
                    .idlePingInterval(config.idlePing)
                    .ackTimeout(config.ackTimeout)
                    .writeQueueBytes(LoadSupport.WRITE_QUEUE_BYTES)
                    .closeFinishDeadline(LoadSupport.CLOSE_FINISH_DEADLINE)
                    .maxFrameSize(LoadSupport.maxFrameSize(config.bodyBytes))
                    .security(security)
                    .authenticate(credentials -> LoadSupport.AUTH_MECHANISM.equals(credentials.mechanism())
                                    && Arrays.equals(LoadSupport.AUTH_SECRET, credentials.secret())
                            ? io.github.ppzxc.strand.netty.AuthenticationResult.accepted(LoadSupport.PRINCIPAL)
                            : io.github.ppzxc.strand.netty.AuthenticationResult.rejected())
                    .handlerFactory(connection -> {
                        if (config.stall) {
                            connection.closed().observe(notice -> announceClosed(out, notice));
                        }
                        return io.github.ppzxc.strand.netty.PreparedHandler.of(
                                request -> {
                                    if (config.stall && request.type() == LoadSupport.STALL_TYPE) {
                                        request.respond(Status.OK, new byte[0]);
                                        for (int i = 0; i < config.inflight; i++) {
                                            push(request.connection(), config.bodyBytes);
                                        }
                                        return;
                                    }
                                    respondAfterDelay(config, () -> request.respond(Status.OK, request.payload()));
                                },
                                () -> {});
                    })
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
