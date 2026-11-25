package io.github.ppzxc.strand.load;

import com.google.gson.JsonObject;
import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.nio.Credentials;
import io.github.ppzxc.strand.nio.NioMinimumProgress;
import io.github.ppzxc.strand.nio.NioSecurity;
import io.github.ppzxc.strand.nio.NioStrandClient;
import io.github.ppzxc.strand.nio.NioStrandConnection;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

final class LoadGenRole {
    private static final int WARMUP = 0;
    private static final int MEASURE = 1;
    private static final int DRAIN = 2;

    private final RunConfig config;
    private final ExecutorService executor = LoadSupport.newExecutor("strand-load-client");
    private final java.util.concurrent.ScheduledExecutorService retry =
            java.util.concurrent.Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "strand-load-client-retry");
                thread.setDaemon(true);
                return thread;
            });
    private final byte[] body;

    private final Request echo;

    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong responded = new AtomicLong();

    private final AtomicLong respondedInWindow = new AtomicLong();

    private final AtomicLong errors = new AtomicLong();
    private final AtomicLong unknown = new AtomicLong();
    private final AtomicLong outstanding = new AtomicLong();
    private final Histogram latency = new ConcurrentHistogram(3_600_000_000L, 3);
    private final Histogram sendLag = new ConcurrentHistogram(3_600_000_000L, 3);

    private static final long BOTTLENECK_LAG_MICROS = 5_000;

    private static final long GIVE_UP_LAG_NANOS = 1_000_000_000L;

    private PrintStream control;
    private JsonObject resourceStart;
    private JsonObject resourceEnd;
    private final java.util.List<JsonObject> soakSamples = java.util.Collections.synchronizedList(new ArrayList<>());
    private volatile boolean sampling;
    private volatile int phase = WARMUP;
    private boolean gaveUp;

    private LoadGenRole(RunConfig config) {
        this.config = config;
        this.body = new byte[config.bodyBytes];
        this.echo = Request.of(LoadSupport.ECHO_TYPE, body);
    }

    static int run(RunConfig config, int port, PrintStream out) throws Exception {
        LoadGenRole role = new LoadGenRole(config);
        role.control = out;
        return role.execute(port, out);
    }

    private int execute(int port, PrintStream out) throws Exception {
        javax.net.ssl.SSLContext tlsContext =
                config.tls ? LoadTls.clientContext(java.nio.file.Paths.get(config.keystore)) : null;
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
        JsonObject negotiated = config.tls ? probeTls(tlsContext, address) : null;
        ClientSide clientSide = config.loadgenTransport.equals("netty")
                ? new NettyClientSide(config, executor, tlsContext)
                : new NioClientSide(config, executor, tlsContext);
        List<LoadConnection> connections = new ArrayList<>();
        for (int i = 0; i < config.connections; i++) {
            LoadConnection connection = clientSide.connect(address);
            awaitServerReady(connection);
            connections.add(connection);
        }

        ResourceSampler.sample();

        for (long v = 1; v < 3_600_000_000L; v = v * 3 / 2 + 1) {
            latency.recordValue(v);
            sendLag.recordValue(v);
        }
        latency.reset();
        sendLag.reset();
        if (config.stall) {
            return runStalledPeer(connections.get(0), out);
        }
        long measureNanos = config.openLoop ? runOpen(connections) : runClosed(connections);
        JsonObject resources = ResourceSampler.delta(resourceStart, resourceEnd);

        JsonObject result = new JsonObject();
        result.addProperty("requests", sent.get());
        result.addProperty("responses", responded.get());
        result.addProperty("windowResponses", respondedInWindow.get());
        result.addProperty("errors", errors.get());
        result.addProperty("unknown", unknown.get());
        result.addProperty("measureNanos", measureNanos);
        result.add("resources", resources);
        if (config.soak) {
            com.google.gson.JsonArray samples = new com.google.gson.JsonArray();
            synchronized (soakSamples) {
                soakSamples.forEach(samples::add);
            }
            result.add("samples", samples);
        }
        if (negotiated != null) {
            result.add("tls", negotiated);
        }
        JsonObject latencyMicros = new JsonObject();
        latencyMicros.addProperty("p50", latency.getValueAtPercentile(50.0));
        latencyMicros.addProperty("p99", latency.getValueAtPercentile(99.0));
        latencyMicros.addProperty("p999", latency.getValueAtPercentile(99.9));
        latencyMicros.addProperty("max", latency.getMaxValue());
        latencyMicros.addProperty("mean", latency.getMean());
        result.add("latencyMicros", latencyMicros);

        result.addProperty("latencyBasis", config.openLoop ? "scheduled" : "send");
        long lagP99 = sendLag.getValueAtPercentile(99.0);
        result.addProperty("sendLagP99Micros", lagP99);
        result.addProperty("loadgenBottleneck", config.openLoop && (gaveUp || lagP99 > BOTTLENECK_LAG_MICROS));
        out.println(result);
        out.flush();

        for (LoadConnection connection : connections) {
            connection.close();
        }
        clientSide.shutdown();
        executor.shutdownNow();
        return 0;
    }

    private static JsonObject probeTls(javax.net.ssl.SSLContext context, InetSocketAddress address) throws Exception {
        try (javax.net.ssl.SSLSocket socket = (javax.net.ssl.SSLSocket)
                context.getSocketFactory().createSocket(address.getAddress(), address.getPort())) {
            javax.net.ssl.SSLParameters parameters = socket.getSSLParameters();
            parameters.setApplicationProtocols(new String[] {LoadTls.ALPN});
            socket.setSSLParameters(parameters);
            socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(LoadSupport.WAIT_SECONDS));
            socket.startHandshake();
            javax.net.ssl.SSLSession session = socket.getSession();
            JsonObject tls = new JsonObject();
            tls.addProperty("protocol", session.getProtocol());
            tls.addProperty("cipherSuite", session.getCipherSuite());
            tls.addProperty("alpn", socket.getApplicationProtocol());
            return tls;
        }
    }

    private void markMeasureStart() throws java.io.IOException {
        resourceStart = ResourceSampler.sample();
        if (config.soak) {
            startSoakSampler();
        }
        control.println("PHASE MEASURE_START");
        control.flush();
    }

    private void markMeasureEnd() throws java.io.IOException {
        resourceEnd = ResourceSampler.sample();
        sampling = false;
        control.println("PHASE MEASURE_END");
        control.flush();
    }

    private int runStalledPeer(LoadConnection connection, PrintStream out) throws Exception {
        connection
                .request(Request.of(LoadSupport.STALL_TYPE, new byte[0]))
                .result()
                .toCompletionStage(executor)
                .toCompletableFuture()
                .get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
        out.println("PEER_READY");
        out.flush();
        byte[] buffer = new byte[64];
        while (System.in.read(buffer) >= 0) {}

        return 0;
    }

    private void startSoakSampler() {
        sampling = true;
        Thread thread = new Thread(() -> {
            while (sampling) {
                try {
                    soakSamples.add(ResourceSampler.sample());
                    Thread.sleep(config.sampleInterval.toMillis());
                } catch (java.io.IOException | InterruptedException e) {
                    return;
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    private long runClosed(List<LoadConnection> connections) throws Exception {
        for (LoadConnection connection : connections) {
            for (int i = 0; i < config.inflight; i++) {
                send(connection, System.nanoTime());
            }
        }
        sleep(config.warmup);
        markMeasureStart();
        long measureStart = System.nanoTime();
        phase = MEASURE;
        sleep(config.measure);
        long measureNanos = System.nanoTime() - measureStart;
        markMeasureEnd();
        phase = DRAIN;
        stopped.set(true);
        drain();
        return measureNanos;
    }

    private long runOpen(List<LoadConnection> connections) throws Exception {
        double intervalNanos = 1e9 / config.rate;
        long start = System.nanoTime();
        long warmupEnd = start + config.warmup.toNanos();
        long measureEnd = warmupEnd + config.measure.toNanos();
        long measureBegin = warmupEnd;
        for (long i = 0; ; i++) {
            long intended = start + (long) (i * intervalNanos);
            if (intended >= measureEnd) {
                break;
            }
            long now = System.nanoTime();
            if (now - intended > GIVE_UP_LAG_NANOS) {
                gaveUp = true;
                measureEnd = now;
                break;
            }
            while ((now = System.nanoTime()) < intended) {
                long remaining = intended - now;
                if (remaining > 200_000) {
                    LockSupport.parkNanos(remaining - 100_000);
                } else {
                    Thread.onSpinWait();
                }
            }
            int next = intended < warmupEnd ? WARMUP : MEASURE;
            if (next == MEASURE && phase != MEASURE) {
                markMeasureStart();
            }
            phase = next;
            send(connections.get((int) (i % connections.size())), intended);
        }
        if (resourceStart == null) {
            markMeasureStart();
        }
        markMeasureEnd();
        phase = DRAIN;
        stopped.set(true);
        drain();

        return Math.max(0, measureEnd - measureBegin);
    }

    private void awaitServerReady(LoadConnection connection) throws Exception {
        long limit =
                System.nanoTime() + Duration.ofSeconds(LoadSupport.WAIT_SECONDS).toNanos();
        long pauseMillis = 5;
        while (true) {
            RequestResult result = connection
                    .request(
                            Request.of(LoadSupport.ECHO_TYPE, body),
                            RequestOptions.deadlineAfter(LoadSupport.PREPARATION_DEADLINE))
                    .result()
                    .toCompletionStage(executor)
                    .toCompletableFuture()
                    .get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            boolean refused = result.status() == Status.UNAVAILABLE && result.certainty() == Certainty.NOT_PROCESSED;
            if (!refused) {
                return;
            }
            if (System.nanoTime() >= limit) {
                throw new IllegalStateException("서버가 준비되지 않았다");
            }
            Thread.sleep(pauseMillis);
            pauseMillis = Math.min(pauseMillis * 2, 100);
        }
    }

    private void send(LoadConnection connection, long intended) {
        if (stopped.get()) {
            return;
        }
        int tag = phase;
        outstanding.incrementAndGet();
        if (tag == MEASURE) {
            sent.incrementAndGet();
            if (config.openLoop) {
                sendLag.recordValue(Math.max(0, (System.nanoTime() - intended) / 1_000));
            }
        }
        connection.request(echo).result().observe(result -> {
            long done = System.nanoTime();
            if (tag == MEASURE && classify(result)) {
                if (phase == MEASURE) {
                    respondedInWindow.incrementAndGet();
                }
                latency.recordValue(Math.max(0, (done - intended) / 1_000));
            }
            boolean failed = result.certainty() != Certainty.RESPONSE || result.status() != Status.OK;
            outstanding.decrementAndGet();
            if (!config.openLoop && !stopped.get()) {

                if (failed) {
                    retry.schedule(
                            () -> executor.execute(() -> send(connection, System.nanoTime())),
                            10,
                            TimeUnit.MILLISECONDS);
                } else {
                    executor.execute(() -> send(connection, System.nanoTime()));
                }
            }
        });
    }

    private boolean classify(RequestResult result) {
        if (result.certainty() == Certainty.UNKNOWN) {
            unknown.incrementAndGet();
            return false;
        }
        if (result.certainty() == Certainty.RESPONSE
                && result.status() == Status.OK
                && result.payload().length == body.length) {
            responded.incrementAndGet();
            return true;
        }
        errors.incrementAndGet();
        return false;
    }

    private void drain() throws InterruptedException {
        long limit =
                System.nanoTime() + Duration.ofSeconds(LoadSupport.WAIT_SECONDS).toNanos();
        while (outstanding.get() > 0 && System.nanoTime() < limit) {
            Thread.sleep(5);
        }
    }

    private static void sleep(Duration duration) throws InterruptedException {
        if (!duration.isZero()) {
            Thread.sleep(duration.toMillis());
        }
    }

    interface LoadConnection {
        io.github.ppzxc.strand.api.RequestHandle request(Request request);

        io.github.ppzxc.strand.api.RequestHandle request(Request request, RequestOptions options);

        void close();
    }

    interface ClientSide {
        LoadConnection connect(InetSocketAddress address) throws Exception;

        void shutdown() throws Exception;
    }

    private static ResourceLimits clientLimits(RunConfig config, MinimumProgressSpace space) {
        return LoadSupport.limits(space, config.connections, config.requestLimit(), config.bodyBytes);
    }

    private static final class NioClientSide implements ClientSide {
        private final NioStrandClient client;

        NioClientSide(RunConfig config, ExecutorService executor, javax.net.ssl.SSLContext tlsContext) {
            NioSecurity security = config.tls ? NioSecurity.tls(tlsContext) : NioSecurity.externallySecured();
            MinimumProgressSpace space = NioMinimumProgress.of(
                    security, LoadSupport.maxFrameSize(config.bodyBytes), config.requestLimit(), config.requestLimit());
            this.client = NioStrandClient.builder()
                    .executor(executor)
                    .resourceLimits(clientLimits(config, space))
                    .preparationDeadline(LoadSupport.PREPARATION_DEADLINE)
                    .shutdownGrace(LoadSupport.SHUTDOWN_GRACE)
                    .shutdownDeadline(LoadSupport.SHUTDOWN_DEADLINE)
                    .writeQueueBytes(LoadSupport.WRITE_QUEUE_BYTES)
                    .closeFinishDeadline(LoadSupport.CLOSE_FINISH_DEADLINE)
                    .maxFrameSize(LoadSupport.maxFrameSize(config.bodyBytes))
                    .security(security)
                    .credentials(Credentials.of(LoadSupport.AUTH_MECHANISM, LoadSupport.AUTH_SECRET))
                    .handler(request -> request.respond(Status.OK, new byte[0]))
                    .build();
        }

        @Override
        public LoadConnection connect(InetSocketAddress address) throws Exception {
            NioStrandConnection connection = client.connectAttempt(address)
                    .result()
                    .toCompletableFuture()
                    .get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            return new LoadConnection() {
                @Override
                public io.github.ppzxc.strand.api.RequestHandle request(Request request) {
                    return connection.request(request);
                }

                @Override
                public io.github.ppzxc.strand.api.RequestHandle request(Request request, RequestOptions options) {
                    return connection.request(request, options);
                }

                @Override
                public void close() {
                    connection.close();
                }
            };
        }

        @Override
        public void shutdown() throws Exception {
            client.shutdown().toCompletableFuture().get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static final class NettyClientSide implements ClientSide {
        private static final int EVENT_LOOPS = 2;
        private final io.netty.channel.EventLoopGroup eventLoops =
                new io.netty.channel.nio.NioEventLoopGroup(EVENT_LOOPS);
        private final io.github.ppzxc.strand.netty.NettyStrandClient client;
        private final io.github.ppzxc.strand.netty.Transport transport;

        NettyClientSide(RunConfig config, ExecutorService executor, javax.net.ssl.SSLContext tlsContext) {
            io.github.ppzxc.strand.netty.NettySecurity security = config.tls
                    ? io.github.ppzxc.strand.netty.NettySecurity.tls(tlsContext)
                    : io.github.ppzxc.strand.netty.NettySecurity.externallySecured();
            MinimumProgressSpace space = io.github.ppzxc.strand.netty.NettyMinimumProgress.of(
                    security, LoadSupport.maxFrameSize(config.bodyBytes), config.requestLimit(), config.requestLimit());
            this.transport = config.binding.equals("websocket")
                    ? io.github.ppzxc.strand.netty.Transport.webSocket(LoadSupport.WS_PATH)
                    : io.github.ppzxc.strand.netty.Transport.tcp();
            this.client = io.github.ppzxc.strand.netty.NettyStrandClient.builder()
                    .eventLoopGroup(eventLoops)
                    .executor(executor)
                    .resourceLimits(clientLimits(config, space))
                    .preparationDeadline(LoadSupport.PREPARATION_DEADLINE)
                    .shutdownGrace(LoadSupport.SHUTDOWN_GRACE)
                    .shutdownDeadline(LoadSupport.SHUTDOWN_DEADLINE)
                    .writeQueueBytes(LoadSupport.WRITE_QUEUE_BYTES)
                    .closeFinishDeadline(LoadSupport.CLOSE_FINISH_DEADLINE)
                    .maxFrameSize(LoadSupport.maxFrameSize(config.bodyBytes))
                    .security(security)
                    .credentials(io.github.ppzxc.strand.netty.Credentials.of(
                            LoadSupport.AUTH_MECHANISM, LoadSupport.AUTH_SECRET))
                    .handler(request -> request.respond(Status.OK, new byte[0]))
                    .build();
        }

        @Override
        public LoadConnection connect(InetSocketAddress address) throws Exception {
            io.github.ppzxc.strand.netty.NettyStrandConnection connection =
                    client.connectAttempt(address, transport).result().get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            return new LoadConnection() {
                @Override
                public io.github.ppzxc.strand.api.RequestHandle request(Request request) {
                    return connection.request(request);
                }

                @Override
                public io.github.ppzxc.strand.api.RequestHandle request(Request request, RequestOptions options) {
                    return connection.request(request, options);
                }

                @Override
                public void close() {
                    connection.close();
                }
            };
        }

        @Override
        public void shutdown() throws Exception {
            client.shutdown().get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            eventLoops.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }
}
