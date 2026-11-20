package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ShutdownDeadlineExceededException;
import io.github.ppzxc.strand.api.ShutdownKind;
import io.github.ppzxc.strand.api.ShutdownResult;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 소유 객체 전체 종료의 순서, 기한 초과 결과의 고정, 실제 자원 정리 관찰, 개별 closed의 완료 경계, 외부 EventLoopGroup 소유권을 앱 공개 API로 검증한다(#326). 기한 시각은
 * 소유 객체가 쓰는 시계(System.nanoTime)로 세므로 기다림은 실제 사건과 그 시각에만 의존한다.
 */
class NettyStrandShutdownTest {

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

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
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

    private NettyStrandServer.Builder server(AppFixture fixture, Duration grace, Duration deadline) {
        return fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .shutdownGrace(grace)
                .shutdownDeadline(deadline);
    }

    private Listener listen(NettyStrandServer server) throws Exception {
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        return listener;
    }

    private NettyStrandConnection connect(NettyStrandClient client, Listener listener) throws Exception {
        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        closeables.add(connection::abort);
        return connection;
    }

    /** 앱 실행기가 작업을 받고도 실행하지 않는 상태를 흉내 낸다. 거절과 달리 예외 없이 삼킨다. */
    private static final class SwallowingExecutor implements Executor {
        private final Executor delegate;
        volatile boolean swallowing;

        SwallowingExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable task) {
            if (!swallowing) {
                delegate.execute(task);
            }
        }
    }

    @Test
    void shutdownStopsIntakeClosesListenersAndConnectionsAndCompletesOnlyAfterTheRealCleanup() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CompletableFuture<NettyStrandConnection> accepted = new CompletableFuture<>();
        NettyStrandServer server = server(serverFixture, Duration.ofSeconds(5), Duration.ofSeconds(10))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> request.respond(Status.OK, bytes("ok")));
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection clientSide = connect(clientFixture.clientBuilder().build(), listener);
        NettyStrandConnection serverSide = accepted.get(15, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(server.shutdownResult().isDone()).isFalse();
        assertThat(server.resourcesTerminated().isDone()).isFalse();

        Future<Void> stopped = server.shutdown();
        AppFixture.get(stopped);

        // 리스닝 중지·연결 CLOSE가 끝났고 실제 자원이 정리된 뒤에야 성공한다.
        // listener.closed()는 앱 실행기를 거쳐 알리므로 종료 완료와 순서가 정해져 있지 않다. 기다려서 확인한다.
        AppFixture.get(listener.closed());
        assertThat(listener.closed().isDone()).isTrue();
        CloseNotice notice = serverFixture.result(serverSide.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.sender()).hasValue(CloseNotice.Sender.LOCAL);
        assertThat(clientFixture.result(clientSide.closed()).kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(server.shutdownResult().isDone()).isTrue();
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
        assertThat(server.shutdownResult().resultNow().remainingResources()).isEmpty();
        AppFixture.get(server.resourcesTerminated());
        assertThat(server.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        // 앱이 소유한 실행기와 EventLoopGroup은 종료하지 않고 계속 쓸 수 있다.
        assertThat(serverFixture.pool.isShutdown()).isFalse();
        assertThat(serverFixture.group.isShuttingDown()).isFalse();
        assertThat(serverFixture.group.next().submit(() -> "alive").get(15, TimeUnit.SECONDS))
                .isEqualTo("alive");
    }

    @Test
    void shutdownReturnsTheSameUncancellableFutureAndResourcesTerminatedIsUncancellable() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        NettyStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> {})
                .build();
        listen(server);

        Future<Void> first = server.shutdown();
        Future<Void> second = server.shutdown();

        assertThat(second).isSameAs(first);
        assertThat(first.cancel(true)).isFalse();
        assertThat(server.resourcesTerminated().cancel(true)).isFalse();
        AppFixture.get(first);
        AppFixture.get(server.resourcesTerminated());
        assertThat(server.shutdown()).isSameAs(first);
        assertThat(server.resourcesTerminated()).isSameAs(server.resourcesTerminated());
    }

    @Test
    void afterShutdownNoNewListenerOrConnectionIsAccepted() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NettyStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = clientFixture.clientBuilder().build();
        AppFixture.get(server.shutdown());
        AppFixture.get(client.shutdown());

        assertThatThrownBy(() -> AppFixture.get(server.listen(AppFixture.anyLoopback())))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        NettyConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener));
        assertThatThrownBy(() -> AppFixture.get(attempt.result()))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PreparationFailedException.class);
        AppFixture.get(attempt.transportReleased());
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).highWater()).isZero();
    }

    @Test
    void aChannelAcceptedAfterShutdownStartedIsClosedWithoutKeepingAnyReservation() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        NettyStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> {})
                .build();
        NettyStrandServer.Acceptor acceptor = server.new Acceptor();
        EmbeddedChannel listening = new EmbeddedChannel(acceptor);
        closeables.add(listening::finishAndReleaseAll);
        ChannelHandlerContext context = listening.pipeline().context(acceptor);
        ServerSocketChannel bound = ServerSocketChannel.open();
        closeables.add(bound::close);
        bound.bind(AppFixture.anyLoopback());
        SocketChannel peer = SocketChannel.open(bound.getLocalAddress());
        closeables.add(peer::close);
        SocketChannel accepted = bound.accept();
        closeables.add(accepted::close);
        AppFixture.get(server.shutdown());

        // 닫기와 경합해 이미 수락된 채널이 전체 종료가 시작된 뒤 도착한다. 연결로 등록하지 않고 소켓을 정리한다.
        acceptor.channelRead(context, new StrandChannel(listening, accepted));

        assertThat(accepted.isOpen()).isFalse();
        assertThat(peer.read(ByteBuffer.allocate(1))).isEqualTo(-1);
        assertThat(server.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(server.resourceUsage(Resource.PREPARING_CONNECTIONS).current())
                .isZero();
    }

    @Test
    void aListenAlreadyInFlightWhenShutdownStartsIsClosedAndShutdownWaitsForIt() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        NettyStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> {})
                .build();
        CountDownLatch release = new CountDownLatch(1);
        for (EventExecutor loop : serverFixture.group) {
            loop.execute(() -> {
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        Future<Listener> listening = server.listen(AppFixture.anyLoopback());

        // 모든 EventLoop가 막혀 바인딩이 시작되지 못했다. 진행 중인 리스닝도 전체 종료가 정리한다.
        Future<Void> stopped = server.shutdown();
        AppFixture.get(stopped);
        AppFixture.get(server.resourcesTerminated());
        release.countDown();

        assertThatThrownBy(() -> AppFixture.get(listening)).isInstanceOf(ExecutionException.class);
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
    }

    @Test
    void clientShutdownCancelsInFlightAttemptsAndClosesEstablishedConnections() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch authEntered = new CountDownLatch(1);
        CompletableFuture<Void> releaseAuth = new CompletableFuture<>();
        NettyStrandServer server = serverFixture
                .serverBuilder()
                .authenticate(c -> {
                    authEntered.countDown();
                    releaseAuth.get(60, TimeUnit.SECONDS);
                    return AuthenticationResult.accepted("p");
                })
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = clientFixture.clientBuilder().build();
        NettyConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener));
        assertThat(authEntered.await(15, TimeUnit.SECONDS)).isTrue();

        AppFixture.get(client.shutdown());
        releaseAuth.complete(null);

        assertThatThrownBy(() -> AppFixture.get(attempt.result()))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PreparationFailedException.class);
        AppFixture.get(attempt.transportReleased());
        assertThat(client.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
        AppFixture.get(client.resourcesTerminated());
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
    }

    @Test
    void anAttemptStillResolvingTheNameIsCancelledAndShutdownWaitsForTheResolverToReturn() throws Exception {
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch resolving = new CountDownLatch(1);
        CountDownLatch releaseResolver = new CountDownLatch(1);
        NettyStrandClient client = clientFixture
                .clientBuilder()
                .shutdownGrace(Duration.ofMillis(100))
                .shutdownDeadline(Duration.ofMillis(500))
                .resolver(host -> {
                    resolving.countDown();
                    try {
                        releaseResolver.await(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return new InetAddress[] {InetAddress.getLoopbackAddress()};
                })
                .build();
        NettyConnectAttempt attempt = client.connectAttempt(InetSocketAddress.createUnresolved("name.invalid", 9));
        assertThat(resolving.await(15, TimeUnit.SECONDS)).isTrue();

        Future<Void> stopped = client.shutdown();

        assertThatThrownBy(() -> AppFixture.get(stopped))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ShutdownDeadlineExceededException.class);
        assertThatThrownBy(() -> AppFixture.get(attempt.result())).isInstanceOf(ExecutionException.class);
        AppFixture.get(attempt.transportReleased());
        // 시도는 끝났지만 주소 해석 작업은 반환할 때까지 잔존 작업이다.
        assertThat(client.shutdownResult().resultNow().remainingResources())
                .containsEntry(Resource.RESOLVE_TASKS, 1L)
                .doesNotContainKey(Resource.TRANSPORTS);
        assertThat(client.resourcesTerminated().isDone()).isFalse();
        releaseResolver.countDown();
        AppFixture.get(client.resourcesTerminated());
        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isZero();
    }

    @Test
    void aConnectionStillPreparingIsEndedByTheGraceAndShutdownWaitsForTheAuthenticationToReturn() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch authEntered = new CountDownLatch(1);
        CompletableFuture<Void> releaseAuth = new CompletableFuture<>();
        NettyStrandServer server = serverFixture
                .serverBuilder()
                .shutdownGrace(Duration.ofMillis(100))
                .shutdownDeadline(Duration.ofMillis(500))
                .authenticate(c -> {
                    authEntered.countDown();
                    releaseAuth.get(60, TimeUnit.SECONDS);
                    return AuthenticationResult.accepted("p");
                })
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        NettyConnectAttempt attempt =
                clientFixture.clientBuilder().build().connectAttempt(AppFixture.loopback(listener));
        assertThat(authEntered.await(15, TimeUnit.SECONDS)).isTrue();

        Future<Void> stopped = server.shutdown();

        assertThatThrownBy(() -> AppFixture.get(stopped))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ShutdownDeadlineExceededException.class);
        // 연결은 끊겼지만 인증 함수가 반환하지 않아 작업 슬롯이 남는다.
        ShutdownResult result = server.shutdownResult().resultNow();
        assertThat(result.remainingResources()).containsEntry(Resource.AUTHENTICATION_TASKS, 1L);
        assertThat(result.remainingResources()).doesNotContainKey(Resource.TRANSPORTS);
        assertThatThrownBy(() -> AppFixture.get(attempt.result())).isInstanceOf(ExecutionException.class);
        assertThat(server.resourcesTerminated().isDone()).isFalse();
        releaseAuth.complete(null);
        AppFixture.get(server.resourcesTerminated());
        assertThat(server.resourceUsage(Resource.AUTHENTICATION_TASKS).current())
                .isZero();
        assertThat(server.shutdownResult().resultNow()).isSameAs(result);
    }

    @Test
    void connectionsWithPendingWorkAreAbortedWhenTheGraceExpires() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CompletableFuture<NettyStrandConnection> serverSide = new CompletableFuture<>();
        NettyStrandServer server = server(serverFixture, Duration.ofMillis(300), Duration.ofSeconds(30))
                .handlerFactory(c -> {
                    serverSide.complete(c);
                    return PreparedHandler.of(r -> {});
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection client =
                connect(clientFixture.clientBuilder().handler(r -> {}).build(), listener);
        // 서버가 서버 쪽 요청을 응답 없이 쥐고 있다. CLOSE는 이 요청이 끝나길 기다리므로 grace 만료 때 abort로 끊긴다.
        NettyStrandConnection accepted = serverSide.get(15, TimeUnit.SECONDS);
        RequestHandle pending = accepted.request(Request.of(TYPE, bytes("never answered")));
        long start = System.nanoTime();

        AppFixture.get(server.shutdown());

        assertThat(System.nanoTime() - start)
                .isGreaterThanOrEqualTo(Duration.ofMillis(300).toNanos());
        RequestResult outcome = serverFixture.result(pending.result());
        assertThat(outcome.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(outcome.certainty()).isNotEqualTo(Certainty.RESPONSE);
        assertThat(serverFixture.result(accepted.closed()).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
        assertThat(client).isNotNull();
    }

    @Test
    void repeatedShutdownReturnsTheSameOutcomeAndNeverExtendsTheDeadline() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        Duration deadline = Duration.ofSeconds(2);
        CountDownLatch handling = new CountDownLatch(1);
        CompletableFuture<Void> releaseHandler = new CompletableFuture<>();
        NettyStrandServer server = server(serverFixture, Duration.ofMillis(100), deadline)
                .handler(request -> {
                    handling.countDown();
                    releaseHandler.get(60, TimeUnit.SECONDS);
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection = connect(clientFixture.clientBuilder().build(), listener);
        clientFixture.requestWhenServed(connection, Request.of(TYPE, bytes("hold")), true);
        assertThat(handling.await(15, TimeUnit.SECONDS)).isTrue();

        long first = System.nanoTime();
        Future<Void> stopped = server.shutdown();
        // 기한의 4분의 3이 지난 시점의 반복 호출이 기한을 그만큼 미룬다면 확정이 첫 호출 기준 기한의 1.75배보다 늦다.
        Thread.sleep(deadline.toMillis() * 3 / 4);
        assertThat(server.shutdown()).isSameAs(stopped);
        awaitTrue(() -> server.shutdownResult().isDone());
        long elapsed = System.nanoTime() - first;

        assertThat(elapsed).isGreaterThanOrEqualTo(deadline.toNanos());
        assertThat(elapsed).isLessThan(deadline.toNanos() * 7 / 4);
        ShutdownResult result = server.shutdownResult().resultNow();
        assertThat(result.kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
        assertThatThrownBy(() -> AppFixture.get(stopped)).hasCauseInstanceOf(ShutdownDeadlineExceededException.class);
        assertThat(server.shutdownResult().resultNow()).isSameAs(result);
        releaseHandler.complete(null);
        AppFixture.get(server.resourcesTerminated());
    }

    @Test
    void aMissedDeadlineIsKeptWithTheRemainingResourcesAndNeverTurnsIntoSuccess() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch handling = new CountDownLatch(1);
        CompletableFuture<Void> releaseHandler = new CompletableFuture<>();
        NettyStrandServer server = server(serverFixture, Duration.ofMillis(100), Duration.ofMillis(500))
                .handler(request -> {
                    handling.countDown();
                    releaseHandler.get(60, TimeUnit.SECONDS);
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection = connect(clientFixture.clientBuilder().build(), listener);
        clientFixture.requestWhenServed(connection, Request.of(TYPE, bytes("hold")), true);
        assertThat(handling.await(15, TimeUnit.SECONDS)).isTrue();

        Future<Void> stopped = server.shutdown();

        assertThatThrownBy(() -> AppFixture.get(stopped))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ShutdownDeadlineExceededException.class);
        ShutdownResult result = server.shutdownResult().resultNow();
        assertThat(result.kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
        assertThat(result.remainingResources()).containsEntry(Resource.HANDLER_TASKS, 1L);
        assertThat(((ShutdownDeadlineExceededException) stopped.cause()).remainingResources())
                .isEqualTo(result.remainingResources());
        // 기한 초과 뒤에도 실제 정리는 계속되고 잔존 작업이 풀려야 resourcesTerminated가 완료한다.
        assertThat(server.resourcesTerminated().isDone()).isFalse();
        releaseHandler.complete(null);
        AppFixture.get(server.resourcesTerminated());
        assertThat(server.shutdownResult().resultNow()).isSameAs(result);
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
        assertThat(stopped.isSuccess()).isFalse();
    }

    @Test
    void resourcesTerminatedWaitsUntilTheAppClaimsAnUnreleasedResult() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch releaseFactory = new CountDownLatch(1);
        AtomicInteger disposals = new AtomicInteger();
        NettyStrandServer server = server(serverFixture, Duration.ofMillis(100), Duration.ofMillis(500))
                .handlerFactory(connection -> {
                    factoryEntered.countDown();
                    releaseFactory.await();
                    return PreparedHandler.of(r -> {}, disposals::incrementAndGet);
                })
                .build();
        Listener listener = listen(server);
        clientFixture.clientBuilder().build().connectAttempt(AppFixture.loopback(listener));
        assertThat(factoryEntered.await(15, TimeUnit.SECONDS)).isTrue();
        Future<Void> stopped = server.shutdown();
        awaitTrue(() -> server.shutdownResult().isDone());
        serverFixture.executor.rejecting = true;
        releaseFactory.countDown();
        awaitTrue(() -> server.unreleasedHandlers().size() == 1);

        assertThat(server.resourcesTerminated().isDone()).isFalse();
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
        assertThat(server.shutdownResult().resultNow().remainingResources()).containsKey(Resource.FACTORY_TASKS);
        Optional<PreparedHandler> owned = server.unreleasedHandlers().get(0).claim();
        assertThat(owned).isPresent();

        AppFixture.get(server.resourcesTerminated());
        assertThat(server.resourceUsage(Resource.FACTORY_TASKS).current()).isZero();
        assertThat(disposals).hasValue(0);
        awaitTrue(stopped::isDone);
        assertThat(stopped.isSuccess()).isFalse();
    }

    @Test
    void shutdownOutcomesAreRetainedEvenWhenTheAppExecutorRejectsNotifications() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        NettyStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        serverFixture.executor.rejecting = true;

        Future<Void> stopped = server.shutdown();

        AppFixture.get(stopped);
        AppFixture.get(server.resourcesTerminated());
        AppFixture.get(listener.closed());
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
    }

    @Test
    void shutdownCompletesEvenWhenTheAppExecutorAcceptsWorkButNeverRunsIt() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        SwallowingExecutor swallowing = new SwallowingExecutor(serverFixture.pool);
        NettyStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(10))
                .executor(swallowing)
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        NettyStrandConnection clientSide = connect(clientFixture.clientBuilder().build(), listener);
        swallowing.swallowing = true;

        Future<Void> stopped = server.shutdown();

        // 공개 완료는 앱 실행기를 거치지 않으므로 실행기가 작업을 삼켜도 막히지 않는다.
        AppFixture.get(stopped);
        AppFixture.get(server.resourcesTerminated());
        AppFixture.get(listener.closed());
        assertThat(server.shutdownResult().isDone()).isTrue();
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
        assertThat(clientFixture.result(clientSide.closed()).kind()).isEqualTo(CloseNotice.Kind.CLOSE);
    }

    @Test
    void closedDoesNotWaitForAHandlerThatNeverReturns() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch handling = new CountDownLatch(1);
        CompletableFuture<Void> releaseHandler = new CompletableFuture<>();
        CompletableFuture<NettyStrandConnection> accepted = new CompletableFuture<>();
        NettyStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> {
                        handling.countDown();
                        releaseHandler.get(60, TimeUnit.SECONDS);
                    });
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection clientSide = connect(clientFixture.clientBuilder().build(), listener);
        clientFixture.requestWhenServed(clientSide, Request.of(TYPE, bytes("hold")), true);
        assertThat(handling.await(15, TimeUnit.SECONDS)).isTrue();
        NettyStrandConnection serverSide = accepted.get(15, TimeUnit.SECONDS);

        serverSide.abort();
        CloseNotice notice = serverFixture.result(serverSide.closed());

        // 통로는 이미 닫혔고 핸들러 작업 슬롯은 핸들러가 반환할 때까지 남는다.
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(server.resourceUsage(Resource.HANDLER_TASKS).current()).isEqualTo(1);
        assertThat(server.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        releaseHandler.complete(null);
        awaitTrue(() -> server.resourceUsage(Resource.HANDLER_TASKS).current() == 0);
    }

    @Test
    void racingCloseAndAbortFromManyThreadsKeepOneRetainedClosedResultAndReleaseEverythingOnce() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NettyStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> r.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = clientFixture.clientBuilder().build();
        List<NettyStrandConnection> connections = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            connections.add(connect(client, listener));
        }
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (NettyStrandConnection connection : connections) {
            for (int i = 0; i < 4; i++) {
                int variant = i;
                Thread thread = new Thread(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                    if (variant % 2 == 0) {
                        connection.close();
                    } else {
                        connection.abort();
                    }
                    connection.close();
                    connection.abort();
                });
                thread.start();
                threads.add(thread);
            }
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS));
        }

        for (NettyStrandConnection connection : connections) {
            CloseNotice notice = clientFixture.result(connection.closed());
            assertThat(clientFixture.result(connection.closed())).isSameAs(notice);
        }
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        AppFixture.get(server.shutdown());
        assertThat(server.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
    }

    @Test
    void concurrentFirstShutdownCallsStartOneShutdownAndShareTheResult() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NettyStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> r.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = listen(server);
        connect(clientFixture.clientBuilder().build(), listener);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Void>> futures = new CopyOnWriteArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                futures.add(server.shutdown());
            });
            thread.start();
            threads.add(thread);
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS));
        }

        assertThat(futures).hasSize(6);
        assertThat(futures).allSatisfy(future -> assertThat(future).isSameAs(futures.get(0)));
        AppFixture.get(futures.get(0));
        AppFixture.get(server.resourcesTerminated());
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
    }

    @Test
    void shutdownCountsFromTheFirstCallEvenWhenTheGraceIsZero() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CompletableFuture<NettyStrandConnection> accepted = new CompletableFuture<>();
        NettyStrandServer server = server(serverFixture, Duration.ZERO, Duration.ofSeconds(5))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(r -> {});
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection clientSide = connect(clientFixture.clientBuilder().build(), listener);
        NettyStrandConnection serverSide = accepted.get(15, TimeUnit.SECONDS);
        clientSide.request(Request.of(TYPE, bytes("unanswered")));

        AppFixture.get(server.shutdown());

        assertThat(serverSide.closed().isDone()).isTrue();
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
        assertThat(((InetSocketAddress) listener.localAddress()).getPort()).isPositive();
    }

    @Test
    void aListenerThatCannotBeClosedYetIsReportedAsRemainingAndCompletesOnceTheLoopRuns() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        NettyStrandServer server = server(serverFixture, Duration.ofMillis(50), Duration.ofMillis(300))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        CountDownLatch release = new CountDownLatch(1);
        for (EventExecutor loop : serverFixture.group) {
            loop.execute(() -> {
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // 리스닝 채널의 EventLoop가 막혀 있어 닫기가 끝나지 못한 채 기한이 지난다.
        Future<Void> stopped = server.shutdown();

        assertThatThrownBy(() -> AppFixture.get(stopped))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ShutdownDeadlineExceededException.class);
        assertThat(server.shutdownResult().resultNow().remainingResources()).containsEntry(Resource.LISTENERS, 1L);
        assertThat(listener.closed().isDone()).isFalse();
        assertThat(server.resourcesTerminated().isDone()).isFalse();
        release.countDown();
        AppFixture.get(listener.closed());
        AppFixture.get(server.resourcesTerminated());
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
    }

    @Test
    void anInterruptedSupervisorStillWaitsForTheRealEventsAndFinishes() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch handling = new CountDownLatch(1);
        CompletableFuture<Void> releaseHandler = new CompletableFuture<>();
        NettyStrandServer server = server(serverFixture, Duration.ofSeconds(60), Duration.ofSeconds(120))
                .handler(request -> {
                    handling.countDown();
                    releaseHandler.get(60, TimeUnit.SECONDS);
                    request.respond(Status.OK, new byte[0]);
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection = connect(clientFixture.clientBuilder().build(), listener);
        clientFixture.requestWhenServed(connection, Request.of(TYPE, bytes("hold")), true);
        assertThat(handling.await(15, TimeUnit.SECONDS)).isTrue();
        java.util.Set<Thread> before = supervisors();
        Future<Void> stopped = server.shutdown();
        // 이 종료가 시작한 감독 스레드만 골라 중단한다(앞선 시험이 남긴 감독 스레드와 구별한다).
        java.util.Set<Thread> started = supervisors();
        started.removeAll(before);
        assertThat(started).hasSize(1);
        Thread supervisor = started.iterator().next();

        supervisor.interrupt();
        // 중단은 기다림을 끝내지 않는다. 핸들러가 응답해 연결이 실제로 끝나는 사건으로 마무리한다.
        releaseHandler.complete(null);

        AppFixture.get(stopped);
        AppFixture.get(server.resourcesTerminated());
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
    }

    private static java.util.Set<Thread> supervisors() {
        java.util.Set<Thread> found = new java.util.HashSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().equals("strand-netty-shutdown") && thread.isAlive()) {
                found.add(thread);
            }
        }
        return found;
    }
}
