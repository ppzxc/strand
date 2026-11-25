package io.github.ppzxc.strand.load;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class LoadSupport {

    static final int ECHO_TYPE = 256;

    static final String WS_PATH = "/strand";

    static final int STALL_TYPE = 257;

    static final String AUTH_MECHANISM = "BEARER";
    static final String PRINCIPAL = "load-client";
    static final byte[] AUTH_SECRET = "load-token".getBytes(StandardCharsets.UTF_8);

    static final Duration PREPARATION_DEADLINE = Duration.ofSeconds(10);
    static final Duration SHUTDOWN_GRACE = Duration.ofSeconds(1);
    static final Duration SHUTDOWN_DEADLINE = Duration.ofSeconds(5);
    static final long WRITE_QUEUE_BYTES = 64L << 20;
    static final Duration CLOSE_FINISH_DEADLINE = Duration.ofSeconds(5);
    static final long WAIT_SECONDS = 30;

    private LoadSupport() {}

    static long maxFrameSize(int bodyBytes) {
        return Math.max(65_536L, 2L * bodyBytes);
    }

    static ResourceLimits limits(MinimumProgressSpace space, int connections, int inflight, int bodyBytes) {
        long[] progress = {
            space.get(Resource.FRAME_ASSEMBLY_BYTES),
            space.get(Resource.CONTROL_OUTPUT_BYTES),
            space.get(Resource.RESULT_EVENT_BYTES),
            space.get(Resource.TLS_PLAINTEXT_BYTES),
            space.get(Resource.TLS_CIPHERTEXT_BYTES),
            space.get(Resource.TLS_CONTROL_BYTES)
        };
        long requests = inflight;
        long bodyBudget = requests * (bodyBytes + 1024L) * 2;
        long preparationBytes = 1 << 16;
        long observations = Math.max(64, 2L * inflight);

        long owners = connections + 1L;
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(requests, bodyBudget)
                        .incomingRequests(requests, bodyBudget)
                        .responseBytes(bodyBudget)
                        .handlerTasks(requests)
                        .observations(observations)
                        .preparationBytes(preparationBytes)
                        .progressBytes(progress[0], progress[1], progress[2], progress[3], progress[4], progress[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(owners)
                        .totalConnections(owners)
                        .preparationTasks(owners, owners, owners)
                        .outgoingRequests(requests * owners, bodyBudget * owners)
                        .incomingRequests(requests * owners, bodyBudget * owners)
                        .responseBytes(bodyBudget * owners)
                        .handlerTasks(requests * owners)
                        .observations(observations * owners)
                        .preparationBytes(preparationBytes * owners)
                        .progressBytes(
                                progress[0] * owners,
                                progress[1] * owners,
                                progress[2] * owners,
                                progress[3] * owners,
                                progress[4] * owners,
                                progress[5] * owners)
                        .build())
                .build();
    }

    static ExecutorService newExecutor(String name) {
        return Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
    }
}
