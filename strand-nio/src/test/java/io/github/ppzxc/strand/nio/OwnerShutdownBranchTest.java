package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ShutdownDeadlineExceededException;
import io.github.ppzxc.strand.api.ShutdownKind;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.OwnerBudgets;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 전체 종료의 기록·기한·잔존 집계 분기를 수동 시계와 막아 둔 수락자로 결정적으로 확인한다. */
class OwnerShutdownBranchTest {
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    private final AppFixture fixture = AppFixture.plain();
    private final CountDownLatch release = new CountDownLatch(1);

    @AfterEach
    void tearDown() {
        release.countDown();
        fixture.close();
    }

    private CommonSettings.Common common(Duration grace, Duration deadline, LongSupplier clock) {
        ProgressSpace space = ProgressSpace.derive(8, 8, EndpointConfig.DEFAULT_MAX_FRAME_SIZE, null);
        return new CommonSettings.Common(
                fixture.executor,
                fixture.limits,
                new OwnerBudgets(fixture.limits),
                space,
                NioSecurity.externallySecured(),
                SECOND,
                SECOND,
                64L << 20,
                EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                8,
                grace,
                deadline,
                clock,
                EndpointConfig.DEFAULT_IDLE_INTERVAL_MS,
                EndpointConfig.DEFAULT_ACK_TIMEOUT_MS,
                null);
    }

    private Listener listener(ServerSocketChannel server, CountDownLatch inAccept) {
        return new Listener(
                server,
                local(server),
                accepted -> {
                    inAccept.countDown();
                    try {
                        release.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    PassageFactory.closeQuietly(accepted);
                },
                fixture.executor,
                TimeUnit.SECONDS.toNanos(5));
    }

    private static java.net.SocketAddress local(ServerSocketChannel server) {
        try {
            return server.getLocalAddress();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private NioStrandConnection connection(CommonSettings.Common common) {
        ConnectionBudgets budgets = common.owner.newConnection(fixture.limits);
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.CLIENT,
                EndpointConfig.defaults().maxConcurrentStreams(8),
                remaining -> {
                    throw new java.io.IOException("열지 않는다");
                },
                new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 1),
                new SerialExecutor(fixture.executor),
                fixture.executor,
                budgets,
                common.space,
                EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                Long.MAX_VALUE,
                SECOND,
                64L << 20,
                System::nanoTime,
                new ConnectionParams.PreparationObserver() {
                    @Override
                    public void ready(NioStrandConnection connection) {}

                    @Override
                    public void failed(PreparationFailure value) {}
                },
                null,
                null,
                null,
                false);
        return new NioStrandConnection(params);
    }

    @Test
    void nothingIsRecordedOnceShutdownHasStarted() throws Exception {
        LongSupplier clock = () -> 0;
        CommonSettings.Common common = common(Duration.ofSeconds(1), Duration.ofSeconds(2), clock);
        OwnerShutdown shutdown = new OwnerShutdown(common, null);
        assertThat(shutdown.accepting()).isTrue();

        shutdown.shutdown().toCompletableFuture().get(15, TimeUnit.SECONDS);

        assertThat(shutdown.accepting()).isFalse();
        assertThat(shutdown.register(connection(common))).isFalse();
        try (ServerSocketChannel server = ServerSocketChannel.open()) {
            server.bind(AppFixture.anyLoopback());
            Listener listener = listener(server, new CountDownLatch(1));
            assertThat(shutdown.register(listener)).isFalse();
            listener.close();
        }
    }

    @Test
    void aStuckListenerAndAnUnreleasedResultAreCountedAsRemainingUntilTheyResolve() throws Exception {
        // 시계는 읽을 때마다 1초씩 간다. 첫 읽기가 시작 시각이고 유예는 0이라 곧바로 abort, 기한 1.5초는 몇 번 읽으면 지난다.
        AtomicLong reads = new AtomicLong();
        LongSupplier clock = () -> reads.getAndIncrement() * SECOND;
        CommonSettings.Common common = common(Duration.ZERO, Duration.ofMillis(1500), clock);
        UnreleasedHandlers unreleased = new UnreleasedHandlers();
        UnreleasedHandler handler = new UnreleasedHandler(PreparedHandler.of(request -> {}), unreleased, () -> {});
        unreleased.add(handler);
        OwnerShutdown shutdown = new OwnerShutdown(common, unreleased);
        CountDownLatch inAccept = new CountDownLatch(1);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(AppFixture.anyLoopback());
        Listener listener = listener(server, inAccept);
        assertThat(shutdown.register(listener)).isTrue();
        try (SocketChannel client = SocketChannel.open(server.getLocalAddress())) {
            assertThat(client.isConnected()).isTrue();
            assertThat(inAccept.await(15, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(() -> shutdown.shutdown().toCompletableFuture().get(15, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ShutdownDeadlineExceededException.class);

            assertThat(shutdown.result().resultNow().kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
            assertThat(shutdown.result().resultNow().remainingResources())
                    .containsEntry(Resource.LISTENERS, 1L)
                    .containsEntry(Resource.UNRELEASED_HANDLERS, 1L);
            assertThat(shutdown.terminated().toCompletableFuture().isDone()).isFalse();

            release.countDown();
            assertThat(handler.claim()).isPresent();
            shutdown.terminated().toCompletableFuture().get(15, TimeUnit.SECONDS);
        }
    }

    @Test
    void anEmptyUnreleasedRegistryDoesNotHoldBackTermination() throws Exception {
        OwnerShutdown shutdown = new OwnerShutdown(
                common(Duration.ofSeconds(1), Duration.ofSeconds(2), () -> 0), new UnreleasedHandlers());

        shutdown.shutdown().toCompletableFuture().get(15, TimeUnit.SECONDS);

        shutdown.terminated().toCompletableFuture().get(15, TimeUnit.SECONDS);
        assertThat(shutdown.result().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
    }

    @Test
    void withoutAnUnreleasedRegistryTheRemainingCountOmitsIt() throws Exception {
        AtomicLong reads = new AtomicLong();
        LongSupplier clock = () -> reads.getAndIncrement() * SECOND;
        OwnerShutdown shutdown = new OwnerShutdown(common(Duration.ZERO, Duration.ofMillis(1500), clock), null);
        CountDownLatch inAccept = new CountDownLatch(1);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(AppFixture.anyLoopback());
        assertThat(shutdown.register(listener(server, inAccept))).isTrue();
        try (SocketChannel client = SocketChannel.open(server.getLocalAddress())) {
            assertThat(client.isConnected()).isTrue();
            assertThat(inAccept.await(15, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(() -> shutdown.shutdown().toCompletableFuture().get(15, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ShutdownDeadlineExceededException.class);

            assertThat(shutdown.result().resultNow().remainingResources()).containsOnlyKeys(Resource.LISTENERS);
            release.countDown();
            shutdown.terminated().toCompletableFuture().get(15, TimeUnit.SECONDS);
        }
    }

    @Test
    void anUnreleasedResultAloneHoldsBackTerminationUntilItIsClaimed() throws Exception {
        AtomicLong reads = new AtomicLong();
        LongSupplier clock = () -> reads.getAndIncrement() * SECOND;
        UnreleasedHandlers unreleased = new UnreleasedHandlers();
        UnreleasedHandler handler = new UnreleasedHandler(PreparedHandler.of(request -> {}), unreleased, () -> {});
        unreleased.add(handler);
        OwnerShutdown shutdown = new OwnerShutdown(common(Duration.ZERO, Duration.ofMillis(1500), clock), unreleased);

        assertThatThrownBy(() -> shutdown.shutdown().toCompletableFuture().get(15, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ShutdownDeadlineExceededException.class);

        assertThat(shutdown.terminated().toCompletableFuture().isDone()).isFalse();
        assertThat(handler.claim()).isPresent();
        shutdown.terminated().toCompletableFuture().get(15, TimeUnit.SECONDS);
    }
}
