package io.github.ppzxc.strand.nio;

import static io.github.ppzxc.strand.nio.AppFixture.TYPE;
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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 소유 객체 전체 종료의 순서, 기한 초과 결과의 고정, 실제 자원 정리 관찰, 개별 closed의 완료 경계를 앱 공개 API로 검증한다(#320). */
class NioStrandShutdownTest {

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

    private static boolean done(CompletionStage<?> stage) {
        return stage.toCompletableFuture().isDone();
    }

    private NioStrandServer.Builder server(AppFixture fixture, Duration grace, Duration deadline) {
        return fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .shutdownGrace(grace)
                .shutdownDeadline(deadline);
    }

    private Listener listen(NioStrandServer server) throws Exception {
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        return listener;
    }

    private NioStrandConnection connect(NioStrandClient client, Listener listener) throws Exception {
        NioStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        closeables.add(connection::abort);
        return connection;
    }

    /** 현재 시각에 건너뛴 시간을 더하는 시계. 기한 계산을 실제로 기다리지 않고 검증한다. */
    private static final class SkewClock implements java.util.function.LongSupplier {
        private final AtomicLong skew = new AtomicLong();

        @Override
        public long getAsLong() {
            return System.nanoTime() + skew.get();
        }

        void advance(Duration duration) {
            skew.addAndGet(duration.toNanos());
        }
    }

    @Test
    void shutdownStopsIntakeClosesListenersAndConnectionsAndCompletesOnlyAfterTheRealCleanup() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CompletableFuture<NioStrandConnection> accepted = new CompletableFuture<>();
        NioStrandServer server = server(serverFixture, Duration.ofSeconds(5), Duration.ofSeconds(10))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> request.respond(Status.OK, bytes("ok")));
                })
                .build();
        Listener listener = listen(server);
        NioStrandConnection clientSide = connect(clientFixture.clientBuilder().build(), listener);
        NioStrandConnection serverSide = accepted.get(15, TimeUnit.SECONDS);
        assertThat(server.shutdownResult().isDone()).isFalse();
        assertThat(done(server.resourcesTerminated())).isFalse();

        CompletionStage<Void> stopped = server.shutdown();
        AppFixture.get(stopped);

        // 리스닝 중지·연결 CLOSE가 끝났고 실제 자원이 정리된 뒤에야 성공한다.
        // listener.closed()는 앱 실행기를 거쳐 알리므로 종료 완료와 순서가 정해져 있지 않다. 기다려서 확인한다.
        AppFixture.get(listener.closed());
        assertThat(done(listener.closed())).isTrue();
        CloseNotice notice = serverFixture.result(serverSide.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.sender()).hasValue(CloseNotice.Sender.LOCAL);
        assertThat(clientFixture.result(clientSide.closed()).kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(server.shutdownResult().isDone()).isTrue();
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
        assertThat(server.shutdownResult().resultNow().remainingResources()).isEmpty();
        AppFixture.get(server.resourcesTerminated());
        assertThat(server.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        // 앱이 준 실행기는 종료하지 않는다.
        assertThat(serverFixture.pool.isShutdown()).isFalse();
    }

    @Test
    void afterShutdownNoNewListenerOrConnectionIsAccepted() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NioStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        NioStrandClient client = clientFixture.clientBuilder().build();
        AppFixture.get(server.shutdown());
        AppFixture.get(client.shutdown());

        assertThatThrownBy(() -> AppFixture.get(server.listen(AppFixture.anyLoopback())))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        NioConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener));
        assertThatThrownBy(() -> AppFixture.get(attempt.result()))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(PreparationFailedException.class);
        AppFixture.get(attempt.transportReleased());
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).highWater()).isZero();
    }

    @Test
    void clientShutdownCancelsInFlightAttemptsAndClosesEstablishedConnections() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch authEntered = new CountDownLatch(1);
        CompletableFuture<Void> releaseAuth = new CompletableFuture<>();
        NioStrandServer server = serverFixture
                .serverBuilder()
                .authenticate(c -> {
                    authEntered.countDown();
                    releaseAuth.get(60, TimeUnit.SECONDS);
                    return AuthenticationResult.accepted("p");
                })
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        NioStrandClient client = clientFixture.clientBuilder().build();
        NioConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener));
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
    void connectionsWithPendingWorkAreAbortedWhenTheGraceExpires() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CompletableFuture<NioStrandConnection> serverSide = new CompletableFuture<>();
        NioStrandServer server = server(serverFixture, Duration.ofMillis(300), Duration.ofSeconds(30))
                .handlerFactory(c -> {
                    serverSide.complete(c);
                    return PreparedHandler.of(r -> {});
                })
                .build();
        Listener listener = listen(server);
        NioStrandConnection client =
                connect(clientFixture.clientBuilder().handler(r -> {}).build(), listener);
        // 서버가 서버 쪽 요청을 응답 없이 쥐고 있다. CLOSE는 이 요청이 끝나길 기다리므로 grace 만료 때 abort로 끊긴다.
        NioStrandConnection accepted = serverSide.get(15, TimeUnit.SECONDS);
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
        SkewClock clock = new SkewClock();
        CountDownLatch handling = new CountDownLatch(1);
        CompletableFuture<Void> releaseHandler = new CompletableFuture<>();
        NioStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(3))
                .nanoClock(clock)
                .handler(request -> {
                    handling.countDown();
                    releaseHandler.get(60, TimeUnit.SECONDS);
                })
                .build();
        Listener listener = listen(server);
        NioStrandConnection connection = connect(clientFixture.clientBuilder().build(), listener);
        clientFixture.requestWhenServed(connection, Request.of(TYPE, bytes("hold")), true);
        assertThat(handling.await(15, TimeUnit.SECONDS)).isTrue();

        CompletionStage<Void> first = server.shutdown();
        clock.advance(Duration.ofSeconds(2));
        CompletionStage<Void> second = server.shutdown();
        assertThat(server.shutdownResult().isDone()).isFalse();
        // 첫 호출 기준 3초가 지났다. 두 번째 호출이 기한을 5초 지점으로 미뤘다면 아직 확정되지 않아야 한다.
        clock.advance(Duration.ofMillis(1500));
        server.shutdown();
        awaitTrue(() -> server.shutdownResult().isDone());

        ShutdownResult result = server.shutdownResult().resultNow();
        assertThat(result.kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
        awaitTrue(() -> done(first) && done(second));
        assertThat(first.toCompletableFuture()).isCompletedExceptionally();
        assertThat(second.toCompletableFuture()).isCompletedExceptionally();
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
        NioStrandServer server = server(serverFixture, Duration.ofMillis(100), Duration.ofMillis(500))
                .handler(request -> {
                    handling.countDown();
                    releaseHandler.get(60, TimeUnit.SECONDS);
                })
                .build();
        Listener listener = listen(server);
        NioStrandConnection connection = connect(clientFixture.clientBuilder().build(), listener);
        clientFixture.requestWhenServed(connection, Request.of(TYPE, bytes("hold")), true);
        assertThat(handling.await(15, TimeUnit.SECONDS)).isTrue();

        CompletionStage<Void> stopped = server.shutdown();

        assertThatThrownBy(() -> AppFixture.get(stopped))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ShutdownDeadlineExceededException.class);
        ShutdownResult result = server.shutdownResult().resultNow();
        assertThat(result.kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
        assertThat(result.remainingResources()).containsEntry(Resource.HANDLER_TASKS, 1L);
        Throwable cause;
        try {
            AppFixture.get(stopped);
            throw new AssertionError("기한 초과여야 한다");
        } catch (ExecutionException e) {
            cause = e.getCause();
        }
        assertThat(((ShutdownDeadlineExceededException) cause).remainingResources())
                .isEqualTo(result.remainingResources());
        // 기한 초과 뒤에도 실제 정리는 계속되고 잔존 작업이 풀려야 resourcesTerminated가 완료한다.
        assertThat(done(server.resourcesTerminated())).isFalse();
        releaseHandler.complete(null);
        AppFixture.get(server.resourcesTerminated());
        assertThat(server.shutdownResult().resultNow()).isSameAs(result);
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
        assertThat(stopped.toCompletableFuture()).isCompletedExceptionally();
    }

    @Test
    void resourcesTerminatedWaitsUntilTheAppClaimsAnUnreleasedResult() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch factoryEntered = new CountDownLatch(1);
        CountDownLatch releaseFactory = new CountDownLatch(1);
        AtomicInteger disposals = new AtomicInteger();
        NioStrandServer server = server(serverFixture, Duration.ofMillis(100), Duration.ofMillis(500))
                .handlerFactory(connection -> {
                    factoryEntered.countDown();
                    releaseFactory.await();
                    return PreparedHandler.of(r -> {}, disposals::incrementAndGet);
                })
                .build();
        Listener listener = listen(server);
        clientFixture.clientBuilder().build().connectAttempt(AppFixture.loopback(listener));
        assertThat(factoryEntered.await(15, TimeUnit.SECONDS)).isTrue();
        CompletionStage<Void> stopped = server.shutdown();
        awaitTrue(() -> server.shutdownResult().isDone());
        serverFixture.executor.rejecting = true;
        releaseFactory.countDown();
        awaitTrue(() -> server.unreleasedHandlers().size() == 1);

        assertThat(done(server.resourcesTerminated())).isFalse();
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.DEADLINE_EXCEEDED);
        Optional<PreparedHandler> owned = server.unreleasedHandlers().get(0).claim();
        assertThat(owned).isPresent();

        AppFixture.get(server.resourcesTerminated());
        assertThat(server.resourceUsage(Resource.FACTORY_TASKS).current()).isZero();
        assertThat(disposals).hasValue(0);
        awaitTrue(() -> done(stopped));
        assertThat(stopped.toCompletableFuture()).isCompletedExceptionally();
    }

    @Test
    void shutdownOutcomesAreRetainedEvenWhenTheAppExecutorRejectsNotifications() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        NioStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        serverFixture.executor.rejecting = true;

        CompletionStage<Void> stopped = server.shutdown();

        AppFixture.get(stopped);
        AppFixture.get(server.resourcesTerminated());
        AppFixture.get(listener.closed());
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
    }

    @Test
    void anExecutorThatAcceptsButNeverRunsTasksStillLetsEveryShutdownStageFinishAfterTheDeadline() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        NioStrandServer server = server(serverFixture, Duration.ofMillis(100), Duration.ofMillis(300))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        AppFixture clientFixture = track(AppFixture.plain());
        NioStrandClient client = clientFixture
                .clientBuilder()
                .shutdownGrace(Duration.ofMillis(100))
                .shutdownDeadline(Duration.ofMillis(300))
                .build();
        NioConnectAttempt attempt = client.connectAttempt(new InetSocketAddress("127.0.0.1", 1));
        serverFixture.executor.swallowing = true;
        clientFixture.executor.swallowing = true;

        CompletionStage<Void> stopped = server.shutdown();

        AppFixture.get(stopped);
        AppFixture.get(server.resourcesTerminated());
        AppFixture.get(listener.closed());
        AppFixture.get(client.shutdown());
        AppFixture.get(attempt.transportReleased());
    }

    @Test
    void shutdownStagesCompleteOnTheAppExecutorWhenItRunsTasks() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NioStrandServer server = server(fixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        List<String> threads = new CopyOnWriteArrayList<>();

        // 완료 전에 붙여야 콜백이 완료한 스레드에서 돈다.
        server.resourcesTerminated()
                .thenRun(() -> threads.add(Thread.currentThread().getName()));
        listener.closed().thenRun(() -> threads.add(Thread.currentThread().getName()));

        server.shutdown();
        awaitTrue(() -> threads.size() == 2);
        assertThat(threads).allMatch(name -> name.startsWith("app-executor-"));
    }

    @Test
    void theServerShutdownStageCompletesOnTheAppExecutorWhenItRunsTasks() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CompletableFuture<NioStrandConnection> serverSide = new CompletableFuture<>();
        NioStrandServer server = server(serverFixture, Duration.ofMillis(300), Duration.ofSeconds(30))
                .handlerFactory(c -> {
                    serverSide.complete(c);
                    return PreparedHandler.of(r -> {});
                })
                .build();
        Listener listener = listen(server);
        connect(clientFixture.clientBuilder().handler(r -> {}).build(), listener);
        // 응답 없는 요청이 grace 만료까지 종료를 붙들어, 아래 콜백은 항상 완료 전에 붙는다.
        serverSide.get(15, TimeUnit.SECONDS).request(Request.of(TYPE, bytes("never answered")));
        List<String> threads = new CopyOnWriteArrayList<>();

        CompletionStage<Void> stopped = server.shutdown();
        assertThat(done(stopped)).isFalse();
        stopped.thenRun(() -> threads.add(Thread.currentThread().getName()));

        AppFixture.get(stopped);
        awaitTrue(() -> threads.size() == 1);
        assertThat(threads).allMatch(name -> name.startsWith("app-executor-"));
    }

    @Test
    void theClientShutdownStageCompletesOnTheAppExecutorWhenItRunsTasks() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CompletableFuture<NioStrandConnection> serverSide = new CompletableFuture<>();
        NioStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(30))
                .handlerFactory(c -> {
                    serverSide.complete(c);
                    return PreparedHandler.of(r -> {});
                })
                .build();
        Listener listener = listen(server);
        NioStrandClient client = clientFixture
                .clientBuilder()
                .shutdownGrace(Duration.ofMillis(300))
                .shutdownDeadline(Duration.ofSeconds(30))
                .handler(r -> {})
                .build();
        NioStrandConnection connection = connect(client, listener);
        connection.request(Request.of(TYPE, bytes("never answered")));
        List<String> threads = new CopyOnWriteArrayList<>();

        CompletionStage<Void> stopped = client.shutdown();
        assertThat(done(stopped)).isFalse();
        stopped.thenRun(() -> threads.add(Thread.currentThread().getName()));

        AppFixture.get(stopped);
        awaitTrue(() -> threads.size() == 1);
        assertThat(threads).allMatch(name -> name.startsWith("app-executor-"));
    }

    @Test
    void transportReleasedCompletesOnTheAppExecutorWhenItRunsTasks() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NioStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> {})
                .build();
        Listener listener = listen(server);
        NioStrandClient client = clientFixture.clientBuilder().build();
        List<String> threads = new CopyOnWriteArrayList<>();

        NioConnectAttempt attempt = client.connectAttempt(AppFixture.loopback(listener));
        attempt.transportReleased()
                .thenRun(() -> threads.add(Thread.currentThread().getName()));

        AppFixture.get(attempt.transportReleased());
        awaitTrue(() -> threads.size() == 1);
        assertThat(threads).allMatch(name -> name.startsWith("app-executor-"));
    }

    @Test
    void closedDoesNotWaitForAHandlerThatNeverReturns() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch handling = new CountDownLatch(1);
        CompletableFuture<Void> releaseHandler = new CompletableFuture<>();
        CompletableFuture<NioStrandConnection> accepted = new CompletableFuture<>();
        NioStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> {
                        handling.countDown();
                        releaseHandler.get(60, TimeUnit.SECONDS);
                    });
                })
                .build();
        Listener listener = listen(server);
        NioStrandConnection clientSide = connect(clientFixture.clientBuilder().build(), listener);
        clientFixture.requestWhenServed(clientSide, Request.of(TYPE, bytes("hold")), true);
        assertThat(handling.await(15, TimeUnit.SECONDS)).isTrue();
        NioStrandConnection serverSide = accepted.get(15, TimeUnit.SECONDS);

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
        NioStrandServer server = server(serverFixture, Duration.ofSeconds(1), Duration.ofSeconds(5))
                .handler(r -> r.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = listen(server);
        NioStrandClient client = clientFixture.clientBuilder().build();
        List<NioStrandConnection> connections = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            connections.add(connect(client, listener));
        }
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (NioStrandConnection connection : connections) {
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

        for (NioStrandConnection connection : connections) {
            CloseNotice notice = clientFixture.result(connection.closed());
            assertThat(clientFixture.result(connection.closed())).isSameAs(notice);
        }
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        AppFixture.get(server.shutdown());
        assertThat(server.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
    }

    @Test
    void shutdownCountsFromTheFirstCallEvenWhenTheGraceIsZero() throws Exception {
        AppFixture serverFixture = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CompletableFuture<NioStrandConnection> accepted = new CompletableFuture<>();
        NioStrandServer server = server(serverFixture, Duration.ZERO, Duration.ofSeconds(5))
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(r -> {});
                })
                .build();
        Listener listener = listen(server);
        NioStrandConnection clientSide = connect(clientFixture.clientBuilder().build(), listener);
        NioStrandConnection serverSide = accepted.get(15, TimeUnit.SECONDS);
        clientSide.request(Request.of(TYPE, bytes("unanswered")));

        AppFixture.get(server.shutdown());

        assertThat(serverSide.closed().isDone()).isTrue();
        assertThat(server.shutdownResult().resultNow().kind()).isEqualTo(ShutdownKind.COMPLETED);
        assertThat(((InetSocketAddress) listener.localAddress()).getPort()).isPositive();
    }
}
