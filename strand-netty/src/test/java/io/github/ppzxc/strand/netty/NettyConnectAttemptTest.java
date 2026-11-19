package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
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

/** 연결 시도의 취소·기한·순차 주소·알림 거절을 Netty 앱 공개 API로 본다(#325). */
class NettyConnectAttemptTest {

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

    private Listener listen(NettyStrandServer server) throws Exception {
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        return listener;
    }

    private static InetAddress loopback(int last) throws java.net.UnknownHostException {
        return InetAddress.getByAddress(new byte[] {127, 0, 0, (byte) last});
    }

    private static NettyStrandServer.Builder accepting(AppFixture fixture) {
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

    private static PreparationFailure failureOf(NettyConnectAttempt attempt) throws Exception {
        return failureOf(AppFixture.cf(attempt.result()));
    }

    private static PreparationFailure failureOf(CompletableFuture<?> stage) throws Exception {
        try {
            stage.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(PreparationFailedException.class);
            return ((PreparationFailedException) e.getCause()).failure();
        }
        throw new AssertionError("연결이 성공했다");
    }

    @Test
    void cancellingAnAttemptStuckInAuthenticationFailsItAsCancelledAndReleasesTheTransport() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch hold = new CountDownLatch(1);
        AtomicInteger factoryRuns = new AtomicInteger();
        NettyStrandServer server = fixture.serverBuilder()
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
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener));
        assertThat(entered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

        assertThat(attempt.cancel()).isTrue();

        PreparationFailure failure = failureOf(attempt);
        assertThat(failure.cause()).isInstanceOf(ConnectionAttemptCancelledException.class);
        AppFixture.get(attempt.transportReleased());
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(client.resourceUsage(Resource.PREPARING_CONNECTIONS).current())
                .isZero();
        // 서버가 끊김을 보고 연결을 정리한 뒤에도 반환하지 않은 사용자 작업은 슬롯을 유지하고, 늦게 반환해도 팩터리를 시작하지 않는다.
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
        NettyStrandServer app = accepting(server)
                .handler(request -> request.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = listen(app);
        NettyStrandClient client = clientFixture.clientBuilder().build();
        NettyConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener));
        NettyStrandConnection connection = AppFixture.get(attempt.result());

        assertThat(attempt.cancel()).isFalse();

        assertThat(clientFixture
                        .result(connection
                                .request(Request.of(AppFixture.TYPE, new byte[0]))
                                .result())
                        .status())
                .isEqualTo(Status.OK);
        // 성공한 시도는 채널을 연결에 넘겼으므로 시도가 정리할 것이 남지 않는다.
        AppFixture.get(attempt.transportReleased());
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isEqualTo(1);
        connection.abort();
    }

    @Test
    void cancellingTheResultFutureDoesNotCancelTheAttempt() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NettyStrandServer app = accepting(server).handler(request -> {}).build();
        Listener listener = listen(app);
        NettyStrandClient client = clientFixture.clientBuilder().build();
        NettyConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener));

        // 관찰 Future의 취소는 시도를 멈추지 못한다. 취소를 받아들이지 않으므로 성공한 연결이 주인 없이 남지도 않는다.
        assertThat(attempt.result().cancel(true)).isFalse();
        assertThat(attempt.transportReleased().cancel(true)).isFalse();

        NettyStrandConnection connection = AppFixture.get(attempt.result());
        assertThat(connection).isNotNull();
        connection.abort();
    }

    @Test
    void addressesAreTriedOneAtATimeInOrderAndTheFirstThatConnectsWins() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NettyStrandServer app = accepting(server)
                .handler(request -> request.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = listen(app);
        int port = ((InetSocketAddress) listener.localAddress()).getPort();
        List<String> tried = new CopyOnWriteArrayList<>();
        // 127.0.0.2에는 아무도 듣지 않으므로 거부되고 다음 주소로 넘어간다.
        NettyStrandClient client = clientFixture
                .clientBuilder()
                .resolver(host -> {
                    tried.add(host);
                    return new InetAddress[] {loopback(2), InetAddress.getLoopbackAddress()};
                })
                .build();

        NettyStrandConnection connection =
                AppFixture.get(client.connect(InetSocketAddress.createUnresolved("some-host", port)));

        // 127.0.0.2는 거부됐으므로 연결이 성공했다면 다음 주소로 맺은 것이다.
        assertThat(tried).containsExactly("some-host");
        assertThat(clientFixture
                        .result(connection
                                .request(Request.of(AppFixture.TYPE, new byte[0]))
                                .result())
                        .status())
                .isEqualTo(Status.OK);
        connection.abort();
    }

    @Test
    void theLastAddressFailureIsReportedWhenEveryAddressFails() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandClient client = fixture.clientBuilder()
                .resolver(host ->
                        new InetAddress[] {loopback(2), loopback(3)})
                .build();

        PreparationFailure failure = failureOf(client.connectAttempt(InetSocketAddress.createUnresolved("h", 1)));

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isInstanceOf(java.net.ConnectException.class);
        assertThat(failure.cause().getMessage()).contains("127.0.0.3");
        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isZero();
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
    }

    @Test
    void theDeadlineIncludesAddressResolutionAndALateResolutionNeverOpensASocket() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        CountDownLatch hold = new CountDownLatch(1);
        try (ServerSocket watcher = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            watcher.setSoTimeout(700);
            NettyStrandClient client = fixture.clientBuilder()
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

            PreparationFailure failure = failureOf(
                    client.connectAttempt(InetSocketAddress.createUnresolved("slow-host", watcher.getLocalPort())));

            assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
            assertThat(failure.cause()).isInstanceOf(PreparationDeadlineExceededException.class);
            awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
            // 반환하지 않은 해석 작업은 슬롯을 유지한다.
            assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isEqualTo(1);
            hold.countDown();
            awaitTrue(() -> client.resourceUsage(Resource.RESOLVE_TASKS).current() == 0);
            try {
                watcher.accept().close();
                throw new AssertionError("늦은 해석 결과로 소켓을 열었다");
            } catch (SocketTimeoutException expected) {
                // 늦은 완료로 연결하지 않는다.
            }
        }
    }

    @Test
    void cancellingWhileResolvingEndsTheAttemptAtOnceAndTheResolveSlotStaysUntilItReturns() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch hold = new CountDownLatch(1);
        NettyStrandClient client = fixture.clientBuilder()
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
        NettyConnectAttempt attempt = client.connectAttempt(InetSocketAddress.createUnresolved("slow-host", 9));
        assertThat(entered.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

        assertThat(attempt.cancel()).isTrue();

        assertThat(failureOf(attempt).cause()).isInstanceOf(ConnectionAttemptCancelledException.class);
        AppFixture.get(attempt.transportReleased());
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isEqualTo(1);
        hold.countDown();
        awaitTrue(() -> client.resourceUsage(Resource.RESOLVE_TASKS).current() == 0);
    }

    @Test
    void anExecutorThatRejectsTheResolutionFailsTheConnectionStageAndReturnsTheSlot() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandClient client = fixture.clientBuilder().build();
        fixture.executor.rejecting = true;

        PreparationFailure failure =
                failureOf(client.connectAttempt(InetSocketAddress.createUnresolved("localhost", 9)));

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isInstanceOf(RejectedExecutionException.class);
        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isZero();
        awaitTrue(() -> client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() == 0);
    }

    @Test
    void aResolverThatThrowsFailsTheConnectionStageWithThatCauseAndReturnsTheSlot() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        IllegalStateException boom = new IllegalStateException("resolver bug");
        NettyStrandClient client = fixture.clientBuilder()
                .resolver(host -> {
                    throw boom;
                })
                .build();

        PreparationFailure failure = failureOf(client.connectAttempt(InetSocketAddress.createUnresolved("h", 9)));

        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isSameAs(boom);
        awaitTrue(() -> client.resourceUsage(Resource.RESOLVE_TASKS).current() == 0);
    }

    @Test
    void whenTheExecutorRejectsTheNotificationTheStageFailsWithTheRejectionAndNoThreadIsSpawned() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NettyStrandServer app = accepting(server).handler(request -> {}).build();
        Listener listener = listen(app);
        NettyStrandClient client = clientFixture.clientBuilder().build();
        clientFixture.executor.rejecting = true;

        PreparationFailure failure = failureOf(client.connectAttempt(AppFixture.loopback(listener)));

        assertThat(rootCauses(failure.cause())).anyMatch(t -> t instanceof RejectedExecutionException);
        // 실행기가 거절한 알림을 대신할 스레드를 만들지 않는다.
        assertThat(Thread.getAllStackTraces().keySet())
                .noneMatch(thread -> thread.getName().equals("strand-netty-notify"));
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
