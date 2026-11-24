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

/**
 * 부하 생성기 프로세스 역할. 전송과 무관하게 항상 같은 코드 경로로 부하를 건다.
 *
 * <ul>
 *   <li>폐쇄형 포화: 연결마다 미완료 요청 {@code inflight}개를 유지하고 하나가 끝나면 곧바로 다음을 보낸다. 처리량 상한을 잰다.
 *   <li>개방형 고정 도착률: 요청의 예정 시각을 미리 정해 두고 서버가 느려도 그 시각에 보낸다. 지연은 예정 시각부터 응답까지로 재므로 서버가 느려도 지연이 축소되어 보이지 않는다(coordinated
 *       omission 방지).
 * </ul>
 *
 * 워밍업 요청은 세지 않는다. 지연은 HdrHistogram(마이크로초)으로 모은다. 결과는 표준 출력에 JSON 한 줄로 낸다.
 */
final class LoadGenRole {
    private static final int WARMUP = 0;
    private static final int MEASURE = 1;
    private static final int DRAIN = 2;

    private final RunConfig config;
    private final ExecutorService executor = LoadSupport.newExecutor("strand-load-client");
    private final byte[] body;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong responded = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final AtomicLong unknown = new AtomicLong();
    private final AtomicLong outstanding = new AtomicLong();
    private final Histogram latency = new ConcurrentHistogram(3_600_000_000L, 3);
    private final Histogram sendLag = new ConcurrentHistogram(3_600_000_000L, 3);
    /** 부하 생성기가 예정 시각을 이만큼 넘기면 병목으로 본다(마이크로초). 개방형에서만 의미가 있다. */
    private static final long BOTTLENECK_LAG_MICROS = 5_000;
    /** 예정보다 이만큼 뒤처지면 더 이상 일정을 지킬 수 없다고 보고 도착을 멈춘다. */
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
    }

    static int run(RunConfig config, int port, PrintStream out) throws Exception {
        LoadGenRole role = new LoadGenRole(config);
        role.control = out;
        return role.execute(port, out);
    }

    private int execute(int port, PrintStream out) throws Exception {
        javax.net.ssl.SSLContext tlsContext =
                config.tls ? LoadTls.clientContext(java.nio.file.Paths.get(config.keystore)) : null;
        NioSecurity security = config.tls ? NioSecurity.tls(tlsContext) : NioSecurity.externallySecured();
        MinimumProgressSpace space = NioMinimumProgress.of(
                security,
                LoadSupport.maxFrameSize(config.bodyBytes),
                config.effectiveInflight(),
                config.effectiveInflight());
        ResourceLimits limits =
                LoadSupport.limits(space, config.connections, config.effectiveInflight(), config.bodyBytes);
        NioStrandClient client = NioStrandClient.builder()
                .executor(executor)
                .resourceLimits(limits)
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
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
        JsonObject negotiated = config.tls ? probeTls(tlsContext, address) : null;
        List<NioStrandConnection> connections = new ArrayList<>();
        for (int i = 0; i < config.connections; i++) {
            NioStrandConnection connection = client.connectAttempt(address)
                    .result()
                    .toCompletableFuture()
                    .get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            awaitServerReady(connection);
            connections.add(connection);
        }

        if (config.stall) {
            return runStalledPeer(connections.get(0), out);
        }
        long measureNanos = config.openLoop ? runOpen(connections) : runClosed(connections);
        JsonObject resources = ResourceSampler.delta(resourceStart, resourceEnd);

        JsonObject result = new JsonObject();
        result.addProperty("requests", sent.get());
        result.addProperty("responses", responded.get());
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
        // 폐쇄형은 보내는 시각 기준(서버가 느리면 축소되어 보일 수 있다), 개방형은 예정 시각 기준이다.
        result.addProperty("latencyBasis", config.openLoop ? "scheduled" : "send");
        long lagP99 = sendLag.getValueAtPercentile(99.0);
        result.addProperty("sendLagP99Micros", lagP99);
        result.addProperty("loadgenBottleneck", config.openLoop && (gaveUp || lagP99 > BOTTLENECK_LAG_MICROS));
        out.println(result);
        out.flush();

        for (NioStrandConnection connection : connections) {
            connection.close();
        }
        client.shutdown().toCompletableFuture().get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
        executor.shutdownNow();
        return 0;
    }

    /**
     * 실제 부하 전에 서버와 TLS 핸드셰이크만 한 번 해 협상된 프로토콜 버전과 암호 스위트를 기록한다. 결과를 전송 사이에 견주어 TLS 조건이 같았는지 확인한다.
     */
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

    /** 측정 구간 경계에서 자원 표본을 뜨고 진행자에게 알린다. 진행자는 이 줄을 보고 서버의 표본을 뜬다. */
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

    /**
     * 쓰기 정체 시나리오의 상대. 연결해 서버가 요청을 밀어 넣게 한 뒤 {@code PEER_READY}를 알리고 기다린다. 진행자가 이 프로세스를 멈춰(SIGSTOP) 읽기를 멈춘 상대를 만든다. 표준 입력이 닫히면 끝난다.
     */
    private int runStalledPeer(NioStrandConnection connection, PrintStream out) throws Exception {
        connection
                .request(Request.of(LoadSupport.STALL_TYPE, new byte[0]))
                .result()
                .toCompletionStage(executor)
                .toCompletableFuture()
                .get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
        out.println("PEER_READY");
        out.flush();
        byte[] buffer = new byte[64];
        while (System.in.read(buffer) >= 0) {
            // 입력은 무시한다.
        }
        return 0;
    }

    /** soak: 측정 구간 동안 일정 간격으로 이 프로세스의 자원 표본을 모은다. */
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

    private long runClosed(List<NioStrandConnection> connections) throws Exception {
        for (NioStrandConnection connection : connections) {
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

    /** 고정 도착률로 예정 시각마다 연결을 돌아가며 보낸다. 늦어도 건너뛰지 않고 바로 보내 밀린 만큼 지연에 드러나게 한다. */
    private long runOpen(List<NioStrandConnection> connections) throws Exception {
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
        return Math.max(1, measureEnd - measureBegin);
    }

    /** 서버 핸들러가 준비되기 전에 온 첫 요청은 거절될 수 있다(규격 4.5절). 거절은 다시 보내도 안전하므로 유한하게 다시 보낸다. */
    private void awaitServerReady(NioStrandConnection connection) throws Exception {
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

    /** @param intended 이 요청의 예정 시각(nanoTime). 폐쇄형은 보내는 시각이다. */
    private void send(NioStrandConnection connection, long intended) {
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
        connection.request(Request.of(LoadSupport.ECHO_TYPE, body)).result().observe(result -> {
            long done = System.nanoTime();
            if (tag == MEASURE && classify(result)) {
                latency.recordValue(Math.max(0, (done - intended) / 1_000));
            }
            outstanding.decrementAndGet();
            if (!config.openLoop) {
                // 전송 스레드를 붙들지 않도록 다음 요청은 앱 실행기에서 보낸다.
                executor.execute(() -> send(connection, System.nanoTime()));
            }
        });
    }

    /** @return 정상 응답이면 true */
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
}
