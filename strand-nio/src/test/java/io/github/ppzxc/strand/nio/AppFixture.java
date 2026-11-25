package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.MinimumProgressSpace;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.TestTls;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class AppFixture implements AutoCloseable {
    static final int TYPE = 0x0100;
    static final long WAIT_SECONDS = 15;

    static final class ToggleExecutor implements java.util.concurrent.Executor {
        private final java.util.concurrent.Executor delegate;
        volatile boolean rejecting;

        volatile boolean swallowing;

        final java.util.concurrent.atomic.AtomicInteger acceptRemaining =
                new java.util.concurrent.atomic.AtomicInteger(Integer.MAX_VALUE);

        ToggleExecutor(java.util.concurrent.Executor delegate) {
            this.delegate = delegate;
        }

        private final java.util.Set<Integer> rejectedNumbers = java.util.concurrent.ConcurrentHashMap.newKeySet();
        private final AtomicInteger submitted = new AtomicInteger();
        private final java.util.List<String> journal = new java.util.concurrent.CopyOnWriteArrayList<>();

        String journal() {
            return String.join("\n", journal);
        }

        private static String caller() {
            StringBuilder where = new StringBuilder();
            for (StackTraceElement e : new Throwable().getStackTrace()) {
                String cls = e.getClassName();
                if (cls.contains("ppzxc") && !cls.contains("AppFixture")) {
                    where.append(cls.substring(cls.lastIndexOf('.') + 1))
                            .append('.')
                            .append(e.getMethodName())
                            .append(':')
                            .append(e.getLineNumber())
                            .append(' ');
                    if (where.length() > 160) {
                        break;
                    }
                }
            }
            return where.toString();
        }

        void rejectTaskNumber(int number) {
            rejectedNumbers.add(number);
        }

        private final java.util.concurrent.atomic.AtomicBoolean rejectNext =
                new java.util.concurrent.atomic.AtomicBoolean();

        void rejectNextTask() {
            rejectNext.set(true);
        }

        private volatile boolean rejectReadyNotification;

        void rejectNextReadyNotification() {
            rejectReadyNotification = true;
        }

        private static boolean fromReadyNotification() {
            for (StackTraceElement e : new Throwable().getStackTrace()) {
                if (e.getMethodName().equals("notifyReady")) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void execute(Runnable task) {
            int number = submitted.incrementAndGet();
            boolean rejectReady = rejectReadyNotification && fromReadyNotification();
            boolean reject = rejectNext.compareAndSet(true, false)
                    || rejectReady
                    || rejecting
                    || acceptRemaining.getAndDecrement() <= 0
                    || rejectedNumbers.contains(number);
            journal.add("#" + number + (reject ? " 거절 " : " 수락 ") + caller());
            if (reject) {
                throw new java.util.concurrent.RejectedExecutionException("테스트가 거절한다");
            }
            if (swallowing) {
                return;
            }
            delegate.execute(task);
        }
    }

    final ExecutorService pool;
    final ToggleExecutor executor;
    final NioSecurity serverSecurity;
    final NioSecurity clientSecurity;
    final boolean tls;
    long maxFrameSize = EndpointConfig.DEFAULT_MAX_FRAME_SIZE;
    final ResourceLimits limits;

    private AppFixture(boolean tls, ResourceLimits limits, NioSecurity serverSecurity, NioSecurity clientSecurity) {
        this.tls = tls;
        this.limits = limits;
        this.serverSecurity = serverSecurity;
        this.clientSecurity = clientSecurity;
        AtomicInteger count = new AtomicInteger();
        ThreadFactory factory = task -> {
            Thread thread = new Thread(task, "app-executor-" + count.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.pool = Executors.newCachedThreadPool(factory);
        this.executor = new ToggleExecutor(pool);
    }

    static AppFixture plain() {
        return plain(8, 8, 4);
    }

    static AppFixture plain(long perConnection, long owner, long connections) {
        return new AppFixture(
                false,
                limits(perConnection, owner, connections, NioSecurity.externallySecured()),
                NioSecurity.externallySecured(),
                NioSecurity.externallySecured());
    }

    static AppFixture tls() throws Exception {
        NioSecurity client = NioSecurity.tls(TestTls.client());
        return new AppFixture(true, limits(8, 8, 4, client), NioSecurity.tls(TestTls.server()), client);
    }

    static final class Spec {
        long perConnection = 8;
        long owner = 8;
        long connections = 4;
        long preparing = -1;
        long authTasks = -1;
        long factoryTasks = -1;
        long ownerIncomingCount = -1;
        long ownerIncomingBytes = 1 << 24;
        long ownerOutgoingBytes = 1 << 24;
        long ownerResponseBytes = 1 << 24;
        long ownerHandlerTasks = -1;
        long connectionHandlerTasks = -1;
        long ownerProgressConnections = -1;
        long perConnectionObservations = 64;
        long maxFrameSize = EndpointConfig.DEFAULT_MAX_FRAME_SIZE;
        long bodyBytes = 1 << 20;
        long ownerObservations = -1;
        long perConnectionPreparationBytes = 1 << 16;
        long ownerPreparationBytes = 1 << 20;

        Spec perConnection(long value) {
            this.perConnection = value;
            return this;
        }

        Spec maxFrameSize(long value) {
            this.maxFrameSize = value;
            return this;
        }

        Spec bodyBytes(long value) {
            this.bodyBytes = value;
            this.ownerIncomingBytes = value * 8;
            this.ownerOutgoingBytes = value * 8;
            this.ownerResponseBytes = value * 8;
            return this;
        }

        Spec preparationBytes(long perConnection, long owner) {
            this.perConnectionPreparationBytes = perConnection;
            this.ownerPreparationBytes = owner;
            return this;
        }

        Spec connections(long value) {
            this.connections = value;
            return this;
        }

        Spec preparing(long value) {
            this.preparing = value;
            return this;
        }

        Spec preparationTasks(long auth, long factory) {
            this.authTasks = auth;
            this.factoryTasks = factory;
            return this;
        }

        Spec ownerIncoming(long count, long bytes) {
            this.ownerIncomingCount = count;
            this.ownerIncomingBytes = bytes;
            return this;
        }

        Spec ownerHandlerTasks(long value) {
            this.ownerHandlerTasks = value;
            return this;
        }

        Spec connectionHandlerTasks(long value) {
            this.connectionHandlerTasks = value;
            return this;
        }

        Spec ownerOutgoingBytes(long value) {
            this.ownerOutgoingBytes = value;
            return this;
        }

        Spec ownerResponseBytes(long value) {
            this.ownerResponseBytes = value;
            return this;
        }

        Spec ownerProgressConnections(long value) {
            this.ownerProgressConnections = value;
            return this;
        }

        Spec observations(long perConnection, long owner) {
            this.perConnectionObservations = perConnection;
            this.ownerObservations = owner;
            return this;
        }

        AppFixture plain() {
            AppFixture fixture = new AppFixture(
                    false,
                    build(NioSecurity.externallySecured()),
                    NioSecurity.externallySecured(),
                    NioSecurity.externallySecured());
            fixture.maxFrameSize = maxFrameSize;
            return fixture;
        }

        AppFixture tls() throws Exception {
            NioSecurity client = NioSecurity.tls(TestTls.client());
            AppFixture fixture = new AppFixture(true, build(client), NioSecurity.tls(TestTls.server()), client);
            fixture.maxFrameSize = maxFrameSize;
            return fixture;
        }

        ResourceLimits build(NioSecurity security) {
            MinimumProgressSpace space = NioMinimumProgress.of(security, maxFrameSize, perConnection, perConnection);
            long[] min = {
                space.get(Resource.FRAME_ASSEMBLY_BYTES),
                space.get(Resource.CONTROL_OUTPUT_BYTES),
                space.get(Resource.RESULT_EVENT_BYTES),
                space.get(Resource.TLS_PLAINTEXT_BYTES),
                space.get(Resource.TLS_CIPHERTEXT_BYTES),
                space.get(Resource.TLS_CONTROL_BYTES)
            };
            long progressConnections = ownerProgressConnections < 0 ? connections : ownerProgressConnections;
            return ResourceLimits.builder()
                    .perConnection(ConnectionLimits.builder()
                            .outgoingRequests(perConnection, bodyBytes)
                            .incomingRequests(perConnection, bodyBytes)
                            .responseBytes(bodyBytes)
                            .handlerTasks(connectionHandlerTasks < 0 ? perConnection : connectionHandlerTasks)
                            .observations(perConnectionObservations)
                            .preparationBytes(perConnectionPreparationBytes)
                            .progressBytes(min[0], min[1], min[2], min[3], min[4], min[5])
                            .build())
                    .perOwner(OwnerLimits.builder()
                            .preparingConnections(preparing < 0 ? connections : preparing)
                            .totalConnections(connections)
                            .preparationTasks(
                                    connections,
                                    authTasks < 0 ? connections : authTasks,
                                    factoryTasks < 0 ? connections : factoryTasks)
                            .outgoingRequests(owner * connections, ownerOutgoingBytes)
                            .incomingRequests(
                                    ownerIncomingCount < 0 ? owner * connections : ownerIncomingCount,
                                    ownerIncomingBytes)
                            .responseBytes(ownerResponseBytes)
                            .handlerTasks(ownerHandlerTasks < 0 ? owner * connections : ownerHandlerTasks)
                            .observations(ownerObservations < 0 ? 64 * connections : ownerObservations)
                            .preparationBytes(ownerPreparationBytes)
                            .progressBytes(
                                    min[0] * progressConnections,
                                    min[1] * progressConnections,
                                    min[2] * progressConnections,
                                    min[3] * progressConnections,
                                    min[4] * progressConnections,
                                    min[5] * progressConnections)
                            .build())
                    .build();
        }
    }

    static ResourceLimits limits(long perConnection, long owner, long connections, NioSecurity security) {
        Spec spec = new Spec().perConnection(perConnection).connections(connections);
        spec.owner = owner;
        return spec.build(security);
    }

    NioStrandServer.Builder serverBuilder() {
        return NioStrandServer.builder()
                .executor(executor)
                .resourceLimits(limits)
                .preparationDeadline(Duration.ofSeconds(10))
                .shutdownGrace(Duration.ofSeconds(1))
                .shutdownDeadline(Duration.ofSeconds(5))
                .maxFrameSize(maxFrameSize)
                .security(serverSecurity);
    }

    NioStrandClient.Builder clientBuilder() {
        return NioStrandClient.builder()
                .executor(executor)
                .resourceLimits(limits)
                .preparationDeadline(Duration.ofSeconds(10))
                .shutdownGrace(Duration.ofSeconds(1))
                .shutdownDeadline(Duration.ofSeconds(5))
                .maxFrameSize(maxFrameSize)
                .security(clientSecurity);
    }

    static InetSocketAddress loopback(Listener listener) {
        return new InetSocketAddress(
                InetAddress.getLoopbackAddress(), ((InetSocketAddress) listener.localAddress()).getPort());
    }

    static InetSocketAddress anyLoopback() {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    static <T> T get(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    <T> T result(Result<T> result) throws Exception {
        return result.toCompletionStage(pool).toCompletableFuture().get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    void requestWhenServed(
            NioStrandConnection connection, io.github.ppzxc.strand.api.Request request, boolean unanswered)
            throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            io.github.ppzxc.strand.api.RequestHandle handle = connection.request(request);
            Thread.sleep(50);
            if (!handle.result().isDone()) {
                return;
            }
            if (handle.result().resultNow().status() != io.github.ppzxc.strand.api.Status.UNAVAILABLE) {
                return;
            }
        }
        throw new AssertionError("서버가 핸들러를 활성화하지 않았다");
    }

    io.github.ppzxc.strand.api.RequestResult requestWhenServed(
            NioStrandConnection connection, io.github.ppzxc.strand.api.Request request) throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            io.github.ppzxc.strand.api.RequestResult result =
                    result(connection.request(request).result());
            if (result.status() != io.github.ppzxc.strand.api.Status.UNAVAILABLE) {
                return result;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("서버가 핸들러를 활성화하지 않았다");
    }

    @Override
    public void close() {
        pool.shutdownNow();
    }
}
