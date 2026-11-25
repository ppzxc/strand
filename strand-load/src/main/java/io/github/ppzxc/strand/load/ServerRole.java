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

final class ServerRole {

    static final int NETTY_EVENT_LOOPS = 2;

    private static final java.util.concurrent.ScheduledExecutorService DELAYS =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "strand-load-server-delay");
                thread.setDaemon(true);
                return thread;
            });

    private ServerRole() {}

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

    private static void respondAfterDelay(RunConfig config, Runnable respond) {
        if (config.serverDelay.isZero()) {
            respond.run();
        } else {
            DELAYS.schedule(respond, config.serverDelay.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    private interface Backend {

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
                    security, LoadSupport.maxFrameSize(config.bodyBytes), config.requestLimit(), config.requestLimit());
            ResourceLimits limits =
                    LoadSupport.limits(space, config.connections, config.requestLimit(), config.bodyBytes);
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
        private final RunConfig config;
        private final io.github.ppzxc.strand.netty.NettyStrandServer server;
        private io.github.ppzxc.strand.netty.Listener listener;

        NettyBackend(RunConfig config, ExecutorService executor, PrintStream out) throws Exception {
            this.config = config;
            io.github.ppzxc.strand.netty.NettySecurity security = config.tls
                    ? io.github.ppzxc.strand.netty.NettySecurity.tls(
                            LoadTls.serverContext(java.nio.file.Paths.get(config.keystore)))
                    : io.github.ppzxc.strand.netty.NettySecurity.externallySecured();
            MinimumProgressSpace space = io.github.ppzxc.strand.netty.NettyMinimumProgress.of(
                    security, LoadSupport.maxFrameSize(config.bodyBytes), config.requestLimit(), config.requestLimit());
            ResourceLimits limits =
                    LoadSupport.limits(space, config.connections, config.requestLimit(), config.bodyBytes);
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
                            config.binding.equals("websocket")
                                    ? io.github.ppzxc.strand.netty.Transport.webSocket(LoadSupport.WS_PATH)
                                    : io.github.ppzxc.strand.netty.Transport.tcp())
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
