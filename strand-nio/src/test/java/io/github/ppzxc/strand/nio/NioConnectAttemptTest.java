package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.Status;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NioConnectAttemptTest {

    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private AppFixture track(AppFixture fixture) {
        closeables.add(fixture);
        return fixture;
    }

    private Listener listen(NioStrandServer server) throws Exception {
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        return listener;
    }

    private static NioStrandServer.Builder accepting(AppFixture fixture) {
        return fixture.serverBuilder().authenticate(credentials -> AuthenticationResult.accepted("peer"));
    }

    private static void awaitTrue(BooleanSupplier condition) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("조건이 시간 안에 참이 되지 않았다");
            }
            Thread.sleep(10);
        }
    }

    private static PreparationFailure failureOf(CompletableFuture<?> stage) throws Exception {
        try {
            stage.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof java.util.concurrent.CompletionException) {
                cause = cause.getCause();
            }
            assertThat(cause).isInstanceOf(PreparationFailedException.class);
            return ((PreparationFailedException) cause).failure();
        }
        throw new AssertionError("연결이 성공했다");
    }

    @Test
    void cancellingAnAttemptStuckInAuthenticationFailsItAsCancelledAndReleasesTheTransport() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch hold = new CountDownLatch(1);
        AtomicInteger factoryRuns = new AtomicInteger();
        NioStrandServer server = fixture.serverBuilder()
                .authenticate(credentials -> {
                    entered.countDown();
                    hold.await();
                    return AuthenticationResult.accepted("p");
                })
                .handlerFactory(connection -> {
                    factoryRuns.incrementAndGet();
                    return PreparedHandler.of(r -> {});
                })
                .build();
        Listener listener = listen(server);
        NioStrandClient client = fixture.clientBuilder().build();
        NioConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener));
        assertThat(entered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

        assertThat(attempt.cancel()).isTrue();

        PreparationFailure failure = failureOf(attempt.result().toCompletableFuture());
        assertThat(failure.cause()).isInstanceOf(ConnectionAttemptCancelledException.class);
        AppFixture.get(attempt.transportReleased());
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(client.resourceUsage(Resource.PREPARING_CONNECTIONS).current())
                .isZero();

        awaitTrue(() -> server.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
        assertThat(server.resourceUsage(Resource.AUTHENTICATION_TASKS).current())
                .isEqualTo(1);
        hold.countDown();
        awaitTrue(() -> server.resourceUsage(Resource.AUTHENTICATION_TASKS).current() == 0);
        assertThat(factoryRuns).hasValue(0);
    }

    @Test
    void aCancelThatArrivesAfterSuccessHasNoEffectAndTheConnectionStaysUsable() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NioStrandServer app = accepting(server)
                .handler(request -> request.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = listen(app);
        NioStrandClient client = clientFixture.clientBuilder().build();
        NioConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener));
        NioStrandConnection connection = AppFixture.get(attempt.result());

        assertThat(attempt.cancel()).isFalse();

        assertThat(clientFixture
                        .result(connection
                                .request(Request.of(AppFixture.TYPE, new byte[0]))
                                .result())
                        .status())
                .isEqualTo(Status.OK);

        AppFixture.get(attempt.transportReleased());
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isEqualTo(1);
        connection.abort();
    }

    @Test
    void addressesAreTriedOneAtATimeInOrderAndTheFirstThatConnectsWins() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NioStrandServer app = accepting(server).handler(request -> {}).build();
        Listener listener = listen(app);
        int port = ((InetSocketAddress) listener.localAddress()).getPort();
        List<String> tried = new CopyOnWriteArrayList<>();

        NioStrandClient client = clientFixture
                .clientBuilder()
                .resolver(host -> {
                    tried.add(host);
                    return new InetAddress[] {InetAddress.getByName("127.0.0.2"), InetAddress.getByName("127.0.0.1")};
                })
                .build();

        NioStrandConnection connection =
                AppFixture.get(client.connect(InetSocketAddress.createUnresolved("some-host", port)));

        assertThat(tried).containsExactly("some-host");
        connection.abort();
    }

    @Test
    void theLastAddressFailureIsReportedWhenEveryAddressFails() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NioStrandClient client = fixture.clientBuilder()
                .resolver(host ->
                        new InetAddress[] {InetAddress.getByName("127.0.0.2"), InetAddress.getByName("127.0.0.3")})
                .build();

        PreparationFailure failure = failureOf(
                client.connect(InetSocketAddress.createUnresolved("h", 1)).toCompletableFuture());

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isInstanceOf(java.net.ConnectException.class);
        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isZero();
    }

    @Test
    void theDeadlineIncludesAddressResolutionAndALateResolutionNeverOpensASocket() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        CountDownLatch hold = new CountDownLatch(1);
        try (ServerSocket watcher = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            watcher.setSoTimeout(700);
            NioStrandClient client = fixture.clientBuilder()
                    .preparationDeadline(Duration.ofMillis(300))
                    .resolver(host -> {
                        try {
                            hold.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return new InetAddress[] {InetAddress.getLoopbackAddress()};
                    })
                    .build();

            PreparationFailure failure =
                    failureOf(client.connect(InetSocketAddress.createUnresolved("slow-host", watcher.getLocalPort()))
                            .toCompletableFuture());

            assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
            assertThat(failure.cause()).isInstanceOf(SocketTimeoutException.class);
            awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);

            assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isEqualTo(1);
            hold.countDown();
            awaitTrue(() -> client.resourceUsage(Resource.RESOLVE_TASKS).current() == 0);
            try {
                watcher.accept().close();
                throw new AssertionError("늦은 해석 결과로 소켓을 열었다");
            } catch (SocketTimeoutException expected) {

            }
        }
    }

    @Test
    void cancellingWhileResolvingEndsTheAttemptAtOnceAndTheResolveSlotStaysUntilItReturns() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch hold = new CountDownLatch(1);
        NioStrandClient client = fixture.clientBuilder()
                .resolver(host -> {
                    entered.countDown();
                    try {
                        hold.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return new InetAddress[] {InetAddress.getLoopbackAddress()};
                })
                .build();
        NioConnectAttempt attempt = client.connectAttempt(InetSocketAddress.createUnresolved("slow-host", 9));
        assertThat(entered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

        assertThat(attempt.cancel()).isTrue();

        assertThat(failureOf(attempt.result().toCompletableFuture()).cause())
                .isInstanceOf(ConnectionAttemptCancelledException.class);
        AppFixture.get(attempt.transportReleased());
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isEqualTo(1);
        hold.countDown();
        awaitTrue(() -> client.resourceUsage(Resource.RESOLVE_TASKS).current() == 0);
    }

    @Test
    void anExecutorThatRejectsTheResolutionFailsTheConnectionStageAndReturnsTheSlot() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NioStrandClient client = fixture.clientBuilder().build();
        fixture.executor.rejecting = true;

        PreparationFailure failure = failureOf(client.connect(InetSocketAddress.createUnresolved("localhost", 9))
                .toCompletableFuture());

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isInstanceOf(RejectedExecutionException.class);
        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isZero();
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
    }

    @Test
    void theClientReservesPreparationBytesBeforeResolvingAndReturnsThemWhenTheConnectionEnds() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NioStrandServer app = accepting(server).handler(request -> {}).build();
        Listener listener = listen(app);
        byte[] secret = "client-secret".getBytes(StandardCharsets.UTF_8);
        NioStrandClient client = clientFixture
                .clientBuilder()
                .credentials(Credentials.of("TOKEN", secret))
                .build();

        NioStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        assertThat(client.resourceUsage(Resource.PREPARATION_BYTES).highWater()).isGreaterThanOrEqualTo(secret.length);
        connection.abort();
        AppFixture.get(connection.closed().toCompletionStage(clientFixture.pool));
        awaitTrue(() -> client.resourceUsage(Resource.PREPARATION_BYTES).current() == 0);
    }

    @Test
    void aClientWithoutEnoughPreparationBytesFailsLocallyAndReservesNothing() throws Exception {
        AppFixture fixture = track(new AppFixture.Spec().preparationBytes(4, 4).plain());
        NioStrandClient client = fixture.clientBuilder()
                .credentials(Credentials.of("TOKEN", "far-too-long-for-four-bytes".getBytes(StandardCharsets.UTF_8)))
                .build();

        PreparationFailure failure =
                failureOf(client.connect(new InetSocketAddress("127.0.0.1", 9)).toCompletableFuture());

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isInstanceOf(ResourceExhaustedException.class);
        assertThat(((ResourceExhaustedException) failure.cause()).resource()).isEqualTo(Resource.PREPARATION_BYTES);
        assertThat(client.resourceUsage(Resource.PREPARATION_BYTES).current()).isZero();
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(client.resourceUsage(Resource.PREPARING_CONNECTIONS).current())
                .isZero();
    }

    @Test
    void theServerReservesTheCredentialBytesBeforeCopyingThemAndReturnsThemWhenTheVerifierReturns() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch hold = new CountDownLatch(1);
        byte[] secret = "server-side-secret".getBytes(StandardCharsets.UTF_8);
        NioStrandServer app = server.serverBuilder()
                .authenticate(credentials -> {
                    entered.countDown();
                    hold.await();
                    return AuthenticationResult.accepted("p");
                })
                .handler(request -> {})
                .build();
        Listener listener = listen(app);
        NioStrandClient client = clientFixture
                .clientBuilder()
                .credentials(Credentials.of("TOKEN", secret))
                .build();
        CompletableFuture<NioStrandConnection> attempt =
                client.connect(AppFixture.loopback(listener)).toCompletableFuture();
        assertThat(entered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

        assertThat(app.resourceUsage(Resource.PREPARATION_BYTES).current()).isEqualTo(secret.length + "TOKEN".length());
        hold.countDown();
        AppFixture.get(attempt).abort();
        awaitTrue(() -> app.resourceUsage(Resource.PREPARATION_BYTES).current() == 0);
    }

    @Test
    void theServerRefusesPreparationWhenTheCredentialBytesDoNotFit() throws Exception {
        AppFixture server = track(new AppFixture.Spec().preparationBytes(4, 4).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        java.util.concurrent.BlockingQueue<PreparationFailure> failures =
                new java.util.concurrent.LinkedBlockingQueue<>();
        AtomicInteger verified = new AtomicInteger();
        NioStrandServer app = server.serverBuilder()
                .authenticate(credentials -> {
                    verified.incrementAndGet();
                    return AuthenticationResult.accepted("p");
                })
                .handler(request -> {})
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);
        NioStrandClient client = clientFixture
                .clientBuilder()
                .credentials(Credentials.of("TOKEN", "far-too-long-for-four-bytes".getBytes(StandardCharsets.UTF_8)))
                .build();
        CompletableFuture<NioStrandConnection> attempt =
                client.connect(AppFixture.loopback(listener)).toCompletableFuture();

        PreparationFailure failure = failures.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);

        assertThat(failure).isNotNull();
        assertThat(failure.stage()).isEqualTo(PreparationStage.AUTHENTICATION);
        assertThat(failure.cause()).isInstanceOf(ResourceExhaustedException.class);
        assertThat(((ResourceExhaustedException) failure.cause()).resource()).isEqualTo(Resource.PREPARATION_BYTES);
        assertThat(verified).hasValue(0);
        assertThat(app.resourceUsage(Resource.AUTHENTICATION_TASKS).current()).isZero();
        assertThat(failureOf(attempt)).isNotNull();
    }

    @Test
    void whenTheExecutorRejectsTheNotificationTheStageFailsWithTheRejectionAndNoThreadIsSpawned() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NioStrandServer app = accepting(server).handler(request -> {}).build();
        Listener listener = listen(app);
        NioStrandClient client = clientFixture.clientBuilder().build();
        clientFixture.executor.rejecting = true;

        PreparationFailure failure =
                failureOf(client.connect(AppFixture.loopback(listener)).toCompletableFuture());

        assertThat(rootCauses(failure.cause())).anyMatch(t -> t instanceof RejectedExecutionException);

        assertThat(Thread.getAllStackTraces().keySet())
                .noneMatch(thread -> thread.getName().equals("strand-nio-notify"));
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
    }

    @Test
    void aPeerRequestCoalescedWithTheAuthAckInOneReadNeverReachesTheClientHandlerAfterTheRejectedNotification()
            throws Exception {
        AppFixture clientFixture = track(AppFixture.plain());
        AtomicInteger handled = new AtomicInteger();
        try (CoalescingPeer peer = new CoalescingPeer(AppFixture.TYPE)) {
            NioStrandClient client = clientFixture
                    .clientBuilder()
                    .handler(request -> {
                        handled.incrementAndGet();
                        request.respond(Status.OK, new byte[0]);
                    })
                    .build();
            clientFixture.executor.rejectNextReadyNotification();
            java.net.InetSocketAddress addr = peer.address();

            PreparationFailure failure = failureOf(client.connect(addr).toCompletableFuture());
            Thread.sleep(300);

            assertThat(rootCauses(failure.cause())).anyMatch(t -> t instanceof RejectedExecutionException);
            assertThat(handled).hasValue(0);
            awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
        }
    }

    @Test
    void aPeerRequestArrivingAfterTheRejectedReadyNotificationNeverReachesTheClientHandler() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        AtomicInteger handled = new AtomicInteger();
        CountDownLatch stop = new CountDownLatch(1);
        NioStrandServer app = accepting(server)
                .handlerFactory(connection -> {
                    Thread sender = new Thread(() -> {
                        try {
                            while (!stop.await(1, TimeUnit.MILLISECONDS)) {
                                connection.request(Request.of(AppFixture.TYPE, new byte[0]));
                            }
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    });
                    sender.setDaemon(true);
                    sender.start();
                    return PreparedHandler.of(request -> {});
                })
                .build();
        Listener listener = listen(app);
        NioStrandClient client = clientFixture
                .clientBuilder()
                .handler(request -> {
                    handled.incrementAndGet();
                    request.respond(Status.OK, new byte[0]);
                })
                .build();
        clientFixture.executor.rejectNextReadyNotification();

        PreparationFailure failure =
                failureOf(client.connect(AppFixture.loopback(listener)).toCompletableFuture());
        Thread.sleep(300);
        stop.countDown();

        assertThat(rootCauses(failure.cause())).anyMatch(t -> t instanceof RejectedExecutionException);
        assertThat(handled).hasValue(0);
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
    }

    @Test
    void aServerWhoseExecutorRejectsTheReadyNotificationKeepsTheConnection() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NioStrandServer app = accepting(server)
                .handlerFactory(connection -> PreparedHandler.of(request -> request.respond(Status.OK, new byte[0])))
                .build();
        Listener listener = listen(app);

        server.executor.rejectTaskNumber(4);

        NioStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));

        assertThat(clientFixture
                        .requestWhenServed(connection, Request.of(AppFixture.TYPE, new byte[0]))
                        .status())
                .isEqualTo(Status.OK);
        connection.abort();
    }

    @Test
    void aResolverThatThrowsFailsTheConnectionStageWithThatCauseAndReturnsTheSlot() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        IllegalStateException boom = new IllegalStateException("resolver bug");
        NioStrandClient client = fixture.clientBuilder()
                .resolver(host -> {
                    throw boom;
                })
                .build();

        PreparationFailure failure = failureOf(
                client.connect(InetSocketAddress.createUnresolved("h", 9)).toCompletableFuture());

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isSameAs(boom);
        awaitTrue(() -> client.resourceUsage(Resource.RESOLVE_TASKS).current() == 0);
    }

    @Test
    void resolvedAddressesAreReservedAsPreparationBytesBeforeTheyAreKept() throws Exception {

        AppFixture fixture = track(new AppFixture.Spec().preparationBytes(4, 4).plain());
        NioStrandClient client = fixture.clientBuilder()
                .resolver(host -> new InetAddress[] {InetAddress.getByName("127.0.0.1")})
                .build();

        PreparationFailure failure = failureOf(
                client.connect(InetSocketAddress.createUnresolved("h", 9)).toCompletableFuture());

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isInstanceOf(ResourceExhaustedException.class);
        assertThat(((ResourceExhaustedException) failure.cause()).resource()).isEqualTo(Resource.PREPARATION_BYTES);
        awaitTrue(() -> client.resourceUsage(Resource.PREPARATION_BYTES).current() == 0);
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
    }

    private static List<Throwable> rootCauses(Throwable failure) {
        List<Throwable> chain = new java.util.ArrayList<>();
        for (Throwable t = failure; t != null; t = t.getCause()) {
            chain.add(t);
        }
        return chain;
    }
}
