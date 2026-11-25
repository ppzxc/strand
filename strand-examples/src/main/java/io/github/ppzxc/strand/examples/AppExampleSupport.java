package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.Status;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class AppExampleSupport {

    static final int ECHO_TYPE = 256;

    static final int HOLD_TYPE = 257;

    static final int REVERSE_TYPE = 258;

    static final int GATED_TYPE = 259;

    static final String AUTH_MECHANISM = "BEARER";
    static final String PRINCIPAL = "demo-client";
    static final byte[] AUTH_SECRET = "demo-token".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    static final long MAX_FRAME_SIZE = 65_536;

    static final long CONNECTIONS = 4;
    static final long REQUESTS_PER_CONNECTION = 8;

    static final long BODY_BYTES = 1 << 20;
    static final long PREPARATION_BYTES = 1 << 16;
    static final long OBSERVATIONS_PER_CONNECTION = 64;

    static final Duration PREPARATION_DEADLINE = Duration.ofSeconds(10);
    static final Duration SHUTDOWN_GRACE = Duration.ofSeconds(1);
    static final Duration SHUTDOWN_DEADLINE = Duration.ofSeconds(5);
    static final long WRITE_QUEUE_BYTES = 64L << 20;
    static final Duration CLOSE_FINISH_DEADLINE = Duration.ofSeconds(5);

    static final Duration REQUEST_DEADLINE = Duration.ofMillis(300);

    static final long WAIT_SECONDS = 15;

    private AppExampleSupport() {}

    static ResourceLimits limits(io.github.ppzxc.strand.api.MinimumProgressSpace space) {
        return limits(new long[] {
            space.get(io.github.ppzxc.strand.api.Resource.FRAME_ASSEMBLY_BYTES),
            space.get(io.github.ppzxc.strand.api.Resource.CONTROL_OUTPUT_BYTES),
            space.get(io.github.ppzxc.strand.api.Resource.RESULT_EVENT_BYTES),
            space.get(io.github.ppzxc.strand.api.Resource.TLS_PLAINTEXT_BYTES),
            space.get(io.github.ppzxc.strand.api.Resource.TLS_CIPHERTEXT_BYTES),
            space.get(io.github.ppzxc.strand.api.Resource.TLS_CONTROL_BYTES)
        });
    }

    static ResourceLimits limits(long[] progress) {
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(REQUESTS_PER_CONNECTION, BODY_BYTES)
                        .incomingRequests(REQUESTS_PER_CONNECTION, BODY_BYTES)
                        .responseBytes(BODY_BYTES)
                        .handlerTasks(REQUESTS_PER_CONNECTION)
                        .observations(OBSERVATIONS_PER_CONNECTION)
                        .preparationBytes(PREPARATION_BYTES)
                        .progressBytes(progress[0], progress[1], progress[2], progress[3], progress[4], progress[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(CONNECTIONS)
                        .totalConnections(CONNECTIONS)
                        .preparationTasks(CONNECTIONS, CONNECTIONS, CONNECTIONS)
                        .outgoingRequests(REQUESTS_PER_CONNECTION * CONNECTIONS, BODY_BYTES * CONNECTIONS)
                        .incomingRequests(REQUESTS_PER_CONNECTION * CONNECTIONS, BODY_BYTES * CONNECTIONS)
                        .responseBytes(BODY_BYTES * CONNECTIONS)
                        .handlerTasks(REQUESTS_PER_CONNECTION * CONNECTIONS)
                        .observations(OBSERVATIONS_PER_CONNECTION * CONNECTIONS)
                        .preparationBytes(PREPARATION_BYTES * CONNECTIONS)
                        .progressBytes(
                                progress[0] * CONNECTIONS,
                                progress[1] * CONNECTIONS,
                                progress[2] * CONNECTIONS,
                                progress[3] * CONNECTIONS,
                                progress[4] * CONNECTIONS,
                                progress[5] * CONNECTIONS)
                        .build())
                .build();
    }

    static ExecutorService newAppExecutor(String name) {
        return Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
    }

    static final class Outcome {
        volatile int echoStatus = -1;
        volatile String echoBody = "";
        volatile String reverseBody = "";
        volatile int reverseStatus = -1;
        volatile int cancelledStatus = -1;
        volatile int deadlineStatus = -1;
        volatile boolean conversionCancelKeptRequest;
        volatile String closeKind = "";
        volatile String serverShutdown = "";
        volatile String clientShutdown = "";
        volatile String authenticatedPrincipal = "";
        volatile boolean stateReleased;
    }

    static RequestResult requestAfterRefusal(Callable<RequestResult> attempt) throws Exception {
        long limit = System.nanoTime() + Duration.ofSeconds(WAIT_SECONDS).toNanos();
        long pauseMillis = 5;
        RequestResult result = attempt.call();
        while (result.status() == Status.UNAVAILABLE
                && result.certainty() == Certainty.NOT_PROCESSED
                && System.nanoTime() < limit) {
            Thread.sleep(pauseMillis);
            pauseMillis = Math.min(pauseMillis * 2, 100);
            result = attempt.call();
        }
        return result;
    }
}
