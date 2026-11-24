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

/**
 * 부하 생성기 프로세스 역할. 전송과 무관하게 항상 같은 코드 경로로 폐쇄형 포화 부하를 건다: 연결마다 미완료 요청 {@code inflight}개를 유지하고 하나가 끝나면 곧바로 다음을 보낸다.
 * 워밍업 요청은 세지 않는다. 결과는 표준 출력에 JSON 한 줄로 낸다.
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
    private volatile int phase = WARMUP;

    private LoadGenRole(RunConfig config) {
        this.config = config;
        this.body = new byte[config.bodyBytes];
    }

    static int run(RunConfig config, int port, PrintStream out) throws Exception {
        return new LoadGenRole(config).execute(port, out);
    }

    private int execute(int port, PrintStream out) throws Exception {
        NioSecurity security = NioSecurity.externallySecured();
        MinimumProgressSpace space = NioMinimumProgress.of(
                security, LoadSupport.maxFrameSize(config.bodyBytes), config.inflight, config.inflight);
        ResourceLimits limits = LoadSupport.limits(space, config.connections, config.inflight, config.bodyBytes);
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
        List<NioStrandConnection> connections = new ArrayList<>();
        for (int i = 0; i < config.connections; i++) {
            NioStrandConnection connection = client.connectAttempt(address)
                    .result()
                    .toCompletableFuture()
                    .get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
            awaitServerReady(connection);
            connections.add(connection);
        }

        for (NioStrandConnection connection : connections) {
            for (int i = 0; i < config.inflight; i++) {
                send(connection);
            }
        }
        sleep(config.warmup);
        long measureStart = System.nanoTime();
        phase = MEASURE;
        sleep(config.measure);
        long measureNanos = System.nanoTime() - measureStart;
        phase = DRAIN;
        stopped.set(true);
        drain();

        JsonObject result = new JsonObject();
        result.addProperty("requests", sent.get());
        result.addProperty("responses", responded.get());
        result.addProperty("errors", errors.get());
        result.addProperty("unknown", unknown.get());
        result.addProperty("measureNanos", measureNanos);
        out.println(result);
        out.flush();

        for (NioStrandConnection connection : connections) {
            connection.close();
        }
        client.shutdown().toCompletableFuture().get(LoadSupport.WAIT_SECONDS, TimeUnit.SECONDS);
        executor.shutdownNow();
        return 0;
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

    private void send(NioStrandConnection connection) {
        if (stopped.get()) {
            return;
        }
        int tag = phase;
        outstanding.incrementAndGet();
        if (tag == MEASURE) {
            sent.incrementAndGet();
        }
        connection.request(Request.of(LoadSupport.ECHO_TYPE, body)).result().observe(result -> {
            if (tag == MEASURE) {
                classify(result);
            }
            outstanding.decrementAndGet();
            // 전송 스레드를 붙들지 않도록 다음 요청은 앱 실행기에서 보낸다.
            executor.execute(() -> send(connection));
        });
    }

    private void classify(RequestResult result) {
        if (result.certainty() == Certainty.UNKNOWN) {
            unknown.incrementAndGet();
        } else if (result.certainty() == Certainty.RESPONSE
                && result.status() == Status.OK
                && result.payload().length == body.length) {
            responded.incrementAndGet();
        } else {
            errors.incrementAndGet();
        }
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
