package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.AuthenticationResult;
import io.github.ppzxc.strand.nio.NioMinimumProgress;
import io.github.ppzxc.strand.nio.NioSecurity;
import io.github.ppzxc.strand.nio.NioStrandClient;
import io.github.ppzxc.strand.nio.NioStrandServer;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class TestNio {

    public static final long REQUESTS_PER_CONNECTION = 128;

    public static final long CONNECTIONS = 64;

    private static final long BODY_BYTES = 1 << 20;
    private static final long OWNER_BODY_BYTES = 1 << 24;

    private TestNio() {}

    public static ExecutorService newExecutor(String name) {
        return Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
    }

    public static ResourceLimits limits() {
        return limits(NioMinimumProgress.of(
                NioSecurity.externallySecured(),
                EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                REQUESTS_PER_CONNECTION,
                REQUESTS_PER_CONNECTION));
    }

    public static ResourceLimits limits(MinimumProgressSpace space) {
        long[] min = {
            space.get(Resource.FRAME_ASSEMBLY_BYTES),
            space.get(Resource.CONTROL_OUTPUT_BYTES),
            space.get(Resource.RESULT_EVENT_BYTES),
            space.get(Resource.TLS_PLAINTEXT_BYTES),
            space.get(Resource.TLS_CIPHERTEXT_BYTES),
            space.get(Resource.TLS_CONTROL_BYTES)
        };
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(REQUESTS_PER_CONNECTION, BODY_BYTES)
                        .incomingRequests(REQUESTS_PER_CONNECTION, BODY_BYTES)
                        .responseBytes(BODY_BYTES)
                        .handlerTasks(REQUESTS_PER_CONNECTION)
                        .observations(64)
                        .preparationBytes(1 << 16)
                        .progressBytes(min[0], min[1], min[2], min[3], min[4], min[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(CONNECTIONS)
                        .totalConnections(CONNECTIONS)
                        .preparationTasks(CONNECTIONS, CONNECTIONS, CONNECTIONS)
                        .outgoingRequests(REQUESTS_PER_CONNECTION * CONNECTIONS, OWNER_BODY_BYTES)
                        .incomingRequests(REQUESTS_PER_CONNECTION * CONNECTIONS, OWNER_BODY_BYTES)
                        .responseBytes(OWNER_BODY_BYTES)
                        .handlerTasks(REQUESTS_PER_CONNECTION * CONNECTIONS)
                        .observations(64 * CONNECTIONS)
                        .preparationBytes(1 << 20)
                        .progressBytes(
                                min[0] * CONNECTIONS,
                                min[1] * CONNECTIONS,
                                min[2] * CONNECTIONS,
                                min[3] * CONNECTIONS,
                                min[4] * CONNECTIONS,
                                min[5] * CONNECTIONS)
                        .build())
                .build();
    }

    public static NioStrandServer.Builder serverBuilder(ExecutorService executor) {
        return NioStrandServer.builder()
                .executor(executor)
                .resourceLimits(limits())
                .preparationDeadline(Duration.ofSeconds(10))
                .shutdownGrace(Duration.ofSeconds(1))
                .shutdownDeadline(Duration.ofSeconds(5))
                .security(NioSecurity.externallySecured())
                .authenticate(credentials -> AuthenticationResult.accepted("peer"));
    }

    public static NioStrandClient.Builder clientBuilder(ExecutorService executor) {
        return NioStrandClient.builder()
                .executor(executor)
                .resourceLimits(limits())
                .preparationDeadline(Duration.ofSeconds(10))
                .shutdownGrace(Duration.ofSeconds(1))
                .shutdownDeadline(Duration.ofSeconds(5))
                .security(NioSecurity.externallySecured());
    }
}
