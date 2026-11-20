package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.AcceptanceKind;
import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Observation;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Status;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 접수 전 예약과 부분 실패 반환, 실행기 거절, 느린 사용자 코드에서도 결과 보존이 계속되는지 본다(#316). */
class NettyStrandReservationTest {

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

    private static NettyStrandServer.Builder acceptingServer(AppFixture fixture) {
        return fixture.serverBuilder().authenticate(c -> AuthenticationResult.accepted("p"));
    }

    @Test
    void receivedRequestsBeyondTheOwnerLimitAreRefusedBeforeAnyHandlerStarts() throws Exception {
        AppFixture server =
                track(new AppFixture.Spec().ownerIncoming(2, 1 << 20).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        AtomicInteger started = new AtomicInteger();
        NettyStrandServer app = acceptingServer(server)
                .handler(request -> started.incrementAndGet())
                .build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        connection.request(Request.of(TYPE, bytes("a")));
        connection.request(Request.of(TYPE, bytes("b")));
        awaitTrue(() -> started.get() == 2);

        RequestResult refused = clientFixture.result(
                connection.request(Request.of(TYPE, bytes("c"))).result());

        // 수신 부족은 핸들러 시작 전 REFUSED이고 RESET(REFUSED)를 받은 요청은 처리 안 됨이다.
        assertThat(refused.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(refused.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(started.get()).isEqualTo(2);
        assertThat(app.resourceUsage(Resource.INCOMING_REQUESTS).rejections()).isEqualTo(1);
        assertThat(app.resourceUsage(Resource.INCOMING_REQUESTS).current()).isEqualTo(2);
    }

    @Test
    void receivedBytesBeyondTheOwnerLimitAreRefusedAndTheCountReservationIsReturned() throws Exception {
        AppFixture server = track(new AppFixture.Spec().ownerIncoming(8, 4).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NettyStrandServer app = acceptingServer(server)
                .handler(request -> request.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));

        RequestResult refused = clientFixture.result(
                connection.request(Request.of(TYPE, bytes("12345"))).result());
        RequestResult accepted = clientFixture.result(
                connection.request(Request.of(TYPE, bytes("1234"))).result());

        assertThat(refused.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(accepted.status()).isEqualTo(Status.OK);
        assertThat(app.resourceUsage(Resource.INCOMING_BYTES).rejections()).isEqualTo(1);
        awaitTrue(() -> app.resourceUsage(Resource.INCOMING_REQUESTS).current() == 0);
        assertThat(app.resourceUsage(Resource.INCOMING_BYTES).current()).isZero();
    }

    @Test
    void handlerTaskSlotsAreReservedBeforeTheHandlerStartsAndReturnedWhenItReturns() throws Exception {
        AppFixture server = track(new AppFixture.Spec().ownerHandlerTasks(1).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch hold = new CountDownLatch(1);
        AtomicInteger started = new AtomicInteger();
        NettyStrandServer app = acceptingServer(server)
                .handler(request -> {
                    started.incrementAndGet();
                    hold.await();
                    request.respond(Status.OK, new byte[0]);
                })
                .build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle first = connection.request(Request.of(TYPE, bytes("a")));
        awaitTrue(() -> started.get() == 1);

        RequestResult refused = clientFixture.result(
                connection.request(Request.of(TYPE, bytes("b"))).result());
        hold.countDown();

        assertThat(refused.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(clientFixture.result(first.result()).status()).isEqualTo(Status.OK);
        awaitTrue(() -> app.resourceUsage(Resource.HANDLER_TASKS).current() == 0);
        assertThat(app.resourceUsage(Resource.HANDLER_TASKS).rejections()).isEqualTo(1);
    }

    @Test
    void anExecutorThatRejectsTheHandlerTaskRefusesTheRequestAndReturnsEveryReservation() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        AtomicInteger started = new AtomicInteger();
        NettyStrandServer app = acceptingServer(server)
                .handler(request -> started.incrementAndGet())
                .build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        server.executor.rejecting = true;

        RequestResult refused = clientFixture.result(
                connection.request(Request.of(TYPE, bytes("a"))).result());

        assertThat(refused.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(refused.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(started.get()).isZero();
        awaitTrue(() -> app.resourceUsage(Resource.INCOMING_REQUESTS).current() == 0);
        assertThat(app.resourceUsage(Resource.HANDLER_TASKS).current()).isZero();
        assertThat(app.resourceUsage(Resource.INCOMING_BYTES).current()).isZero();
    }

    @Test
    void outgoingBytesBeyondTheOwnerLimitFailLocallyAndReturnTheCountReservation() throws Exception {
        AppFixture fixture = track(new AppFixture.Spec().ownerOutgoingBytes(10).plain());
        NettyStrandServer server = acceptingServer(fixture).handler(r -> {}).build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        RequestResult rejected = fixture.result(
                connection.request(Request.of(TYPE, new byte[11])).result());

        assertThat(rejected.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(rejected.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(rejected.resource()).hasValue(Resource.OUTGOING_BYTES);
        assertThat(client.resourceUsage(Resource.OUTGOING_REQUESTS).current()).isZero();
        assertThat(client.resourceUsage(Resource.OUTGOING_BYTES).rejections()).isEqualTo(1);
    }

    @Test
    void aRequestLargerThanTheNegotiatedFrameSizeFailsLocallyWithoutSendingAnything() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandServer server =
                acceptingServer(fixture).maxFrameSize(16384).handler(r -> {}).build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        RequestResult rejected = fixture.result(
                connection.request(Request.of(TYPE, new byte[20000])).result());

        assertThat(rejected.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(rejected.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(rejected.resource()).hasValue(Resource.PEER_FRAME_SIZE);
        assertThat(client.resourceUsage(Resource.OUTGOING_REQUESTS).current()).isZero();
        assertThat(client.resourceUsage(Resource.OUTGOING_BYTES).current()).isZero();
    }

    @Test
    void aResponseWithoutEnoughReservationKeepsNeitherTheBodyNorTheRightToRespond() throws Exception {
        AppFixture server = track(new AppFixture.Spec().ownerResponseBytes(4).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CompletableFuture<IncomingRequest> seen = new CompletableFuture<>();
        NettyStrandServer app = acceptingServer(server).handler(seen::complete).build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("q")));
        IncomingRequest request = seen.get(15, TimeUnit.SECONDS);

        ResponseAcceptance tooBig = server.result(request.respond(Status.OK, bytes("12345")));
        ResponseAcceptance retried = server.result(request.respond(Status.OK, bytes("1234")));
        ResponseAcceptance again = server.result(request.respond(Status.OK, bytes("12")));

        assertThat(tooBig.kind()).isEqualTo(AcceptanceKind.RESOURCE_EXHAUSTED);
        assertThat(tooBig.resource()).hasValue(Resource.RESPONSE_BYTES);
        // 자원 부족은 응답 권한을 소비하지 않았으므로 살아 있는 요청에서 다시 응답할 수 있고, 최초 유효 응답만 접수한다.
        assertThat(retried.kind()).isEqualTo(AcceptanceKind.ACCEPTED);
        assertThat(again.kind()).isEqualTo(AcceptanceKind.ALREADY_RESPONDED);
        assertThat(clientFixture.result(handle.result()).payload()).isEqualTo(bytes("1234"));
        // 응답 바이트는 응답 프레임이 실제로 다 쓰였다고 보고된 뒤 돌아온다(#318 #307). 상대가 응답을 받은 시점과 순서 보장이 없으므로 반환을 기다린다.
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (app.resourceUsage(Resource.RESPONSE_BYTES).current() != 0 && System.nanoTime() < limit) {
            Thread.sleep(5);
        }
        assertThat(app.resourceUsage(Resource.RESPONSE_BYTES).current()).isZero();
    }

    @Test
    void aResponseLargerThanThePeerFrameSizeIsReportedAndDoesNotConsumeTheRightToRespond() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CompletableFuture<IncomingRequest> seen = new CompletableFuture<>();
        NettyStrandServer app = acceptingServer(server).handler(seen::complete).build();
        Listener listener = listen(app);
        NettyStrandConnection connection = AppFixture.get(
                clientFixture.clientBuilder().maxFrameSize(16384).build().connect(AppFixture.loopback(listener)));
        connection.request(Request.of(TYPE, bytes("q")));
        IncomingRequest request = seen.get(15, TimeUnit.SECONDS);

        ResponseAcceptance tooBig = server.result(request.respond(Status.OK, new byte[20000]));
        ResponseAcceptance small = server.result(request.respond(Status.OK, bytes("ok")));

        assertThat(tooBig.kind()).isEqualTo(AcceptanceKind.RESOURCE_EXHAUSTED);
        assertThat(tooBig.resource()).hasValue(Resource.PEER_FRAME_SIZE);
        assertThat(small.kind()).isEqualTo(AcceptanceKind.ACCEPTED);
    }

    @Test
    void respondingToARequestThePeerAlreadyCancelledReportsRequestEnded() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        BlockingQueue<IncomingRequest> seen = new LinkedBlockingQueue<>();
        NettyStrandServer app = acceptingServer(server).handler(seen::add).build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle first = connection.request(Request.of(TYPE, bytes("first")));
        IncomingRequest cancelled = seen.poll(15, TimeUnit.SECONDS);
        first.cancel();
        // RESET은 뒤따르는 요청보다 먼저 처리되므로 두 번째 요청이 핸들러에 닿았을 때 첫 요청은 이미 끝나 있다.
        connection.request(Request.of(TYPE, bytes("second")));
        assertThat(seen.poll(15, TimeUnit.SECONDS)).isNotNull();

        ResponseAcceptance acceptance = server.result(cancelled.respond(Status.OK, bytes("late")));

        assertThat(acceptance.kind()).isEqualTo(AcceptanceKind.REQUEST_ENDED);
        assertThat(app.resourceUsage(Resource.INCOMING_REQUESTS).current()).isEqualTo(1);
    }

    @Test
    void invalidStatusIsRejectedImmediately() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        CompletableFuture<IncomingRequest> seen = new CompletableFuture<>();
        NettyStrandServer server =
                acceptingServer(fixture).handler(seen::complete).build();
        Listener listener = listen(server);
        NettyStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        connection.request(Request.of(TYPE, bytes("q")));
        IncomingRequest request = seen.get(15, TimeUnit.SECONDS);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> request.respond(256, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> request.respond(-1, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void observationsThatCannotBeRegisteredFailWithoutChangingTheRetainedResult() throws Exception {
        AppFixture fixture = track(new AppFixture.Spec().observations(1, 64).plain());
        NettyStrandServer server =
                acceptingServer(fixture).handler(request -> {}).build();
        Listener listener = listen(server);
        NettyStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("slow")));
        CountDownLatch release = new CountDownLatch(1);
        Observation first = handle.result().observe(result -> await(release));
        Observation second = handle.result().observe(result -> {});

        assertThat(second.isDone()).isTrue();
        assertThat(second.failureNow()).get().isInstanceOf(ResourceExhaustedException.class);
        handle.cancel();
        // 관찰 한도가 차 있어도 결과는 확정되어 조회된다(결과 보존은 관찰 실행을 기다리지 않는다).
        awaitTrue(() -> handle.result().isDone());
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.CANCELLED);
        assertThat(first.isDone()).isFalse();
        release.countDown();
        awaitTrue(first::isDone);
        assertThat(first.failureNow()).isEmpty();
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    void aSlowObserverDelaysOnlyItselfBehindTheConnectionSerialLaneNeverTheResult() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        CountDownLatch handlerEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<NettyStrandConnection> accepted = new CompletableFuture<>();
        NettyStrandServer server = acceptingServer(fixture)
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> request.respond(Status.OK, new byte[0]));
                })
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection = AppFixture.get(fixture.clientBuilder()
                .handler(request -> {
                    handlerEntered.countDown();
                    release.await();
                    request.respond(Status.OK, new byte[0]);
                })
                .build()
                .connect(AppFixture.loopback(listener)));
        // 서버가 요청을 내 클라이언트 핸들러를 붙잡아 둔다(직렬 실행 칸이 막힌다).
        accepted.get(15, TimeUnit.SECONDS).request(Request.of(TYPE, bytes("hold")));
        assertThat(handlerEntered.await(15, TimeUnit.SECONDS)).isTrue();
        AtomicInteger observed = new AtomicInteger();
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("x")));
        handle.result().observe(result -> observed.incrementAndGet());

        awaitTrue(() -> handle.result().isDone());

        // 결과는 이미 보존되어 조회되지만 기본 관찰은 같은 연결의 핸들러 뒤에서 기다린다.
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.OK);
        assertThat(observed.get()).isZero();
        release.countDown();
        awaitTrue(() -> observed.get() == 1);
    }

    @Test
    void whenTheAppExecutorRejectsEverythingAfterConnectResultsAreRetainedAndOnlyObservationsFail() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NettyStrandServer app = acceptingServer(server)
                .handler(request -> request.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = listen(app);
        NettyStrandClient client = clientFixture.clientBuilder().build();

        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        // 연결이 준비된 뒤에는 어떤 작업도 받지 않는다. 결과는 보존되고 관찰만 거절된다.
        clientFixture.executor.rejecting = true;
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("x")));
        awaitTrue(() -> handle.result().isDone());
        Observation rejected = handle.result().observe(result -> {});

        assertThat(handle.result().resultNow().status()).isEqualTo(Status.OK);
        assertThat(rejected.isDone()).isTrue();
        assertThat(rejected.failureNow()).get().isInstanceOf(RejectedExecutionException.class);
        connection.abort();
        awaitTrue(() -> connection.closed().isDone());
        assertThat(connection.closed().resultNow()).isNotNull();
    }

    @Test
    void anExecutorThatRejectsTheAuthenticationTaskFailsThePreparationAtThatStage() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        BlockingQueue<PreparationFailure> failures = new LinkedBlockingQueue<>();
        NettyStrandServer app = acceptingServer(server)
                .handler(request -> {})
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);
        // 인증 작업만 거절한다. 실패 알림은 받는다.
        server.executor.rejectTaskNumber(2);

        CompletableFuture<NettyStrandConnection> stage =
                AppFixture.cf(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));

        PreparationFailure failure = failures.poll(15, TimeUnit.SECONDS);
        assertThat(failure).isNotNull();
        assertThat(failure.stage()).isEqualTo(PreparationStage.AUTHENTICATION);
        assertThat(failure.cause()).isInstanceOf(RejectedExecutionException.class);
        assertThat(app.resourceUsage(Resource.AUTHENTICATION_TASKS).current()).isZero();
        stage.cancel(true);
    }

    @Test
    void anExecutorThatRejectsTheFactoryTaskFailsThePreparationAtTheFactoryStage() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        BlockingQueue<PreparationFailure> failures = new LinkedBlockingQueue<>();
        NettyStrandServer app = server.serverBuilder()
                .authenticate(c -> {
                    // 인증 작업이 도는 중에 다음 작업(팩터리)만 거절하도록 건다. 작업 번호에 기대지 않는다.
                    server.executor.rejectNextTask();
                    return AuthenticationResult.accepted("p");
                })
                .handlerFactory(connection -> PreparedHandler.of(request -> {}))
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);

        // 인증 작업과 팩터리는 같은 직렬 실행기 위에서 돈다. 인증 작업이 아직 끝나지 않았으면 팩터리는 새 작업으로 실행기에 제출되지 않고 같은 drain에
        // 얹히므로 거절 대상이 아니다(그때는 연결이 정상 준비된다). 거절이 팩터리 제출에 닿은 시도가 나올 때까지 연결을 다시 시도한다.
        PreparationFailure failure = null;
        for (int attempt = 0; attempt < 50 && failure == null; attempt++) {
            AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
            failure = failures.poll(500, TimeUnit.MILLISECONDS);
        }
        if (failure == null) {
            failure = failures.poll(15, TimeUnit.SECONDS);
        }

        assertThat(failure).as("실행기가 받은 작업:\n" + server.executor.journal()).isNotNull();
        assertThat(failure.stage()).isEqualTo(PreparationStage.HANDLER_FACTORY);
        assertThat(failure.cause()).isInstanceOf(RejectedExecutionException.class);
        assertThat(app.resourceUsage(Resource.FACTORY_TASKS).current()).isZero();
    }

    @Test
    void preparationTaskSlotsBoundBlockedAuthenticationsAndFactories() throws Exception {
        AppFixture server = track(new AppFixture.Spec().preparationTasks(1, 1).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch hold = new CountDownLatch(1);
        BlockingQueue<PreparationFailure> failures = new LinkedBlockingQueue<>();
        NettyStrandServer app = server.serverBuilder()
                .authenticate(credentials -> {
                    hold.await();
                    return AuthenticationResult.accepted("p");
                })
                .handler(request -> {})
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);
        NettyStrandClient client = clientFixture.clientBuilder().build();
        CompletableFuture<NettyStrandConnection> first = AppFixture.cf(client.connect(AppFixture.loopback(listener)));
        awaitTrue(() -> app.resourceUsage(Resource.AUTHENTICATION_TASKS).current() == 1);

        CompletableFuture<NettyStrandConnection> second = AppFixture.cf(client.connect(AppFixture.loopback(listener)));
        PreparationFailure failure = failures.poll(15, TimeUnit.SECONDS);

        assertThat(failure).isNotNull();
        assertThat(failure.stage()).isEqualTo(PreparationStage.AUTHENTICATION);
        assertThat(failure.cause()).isInstanceOf(ResourceExhaustedException.class);
        assertThat(((ResourceExhaustedException) failure.cause()).resource()).isEqualTo(Resource.AUTHENTICATION_TASKS);
        hold.countDown();
        AppFixture.get(first).abort();
        second.cancel(true);
    }

    @Test
    void factoryTaskSlotsBoundBlockedFactories() throws Exception {
        AppFixture server = track(new AppFixture.Spec().preparationTasks(2, 1).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch hold = new CountDownLatch(1);
        BlockingQueue<PreparationFailure> failures = new LinkedBlockingQueue<>();
        NettyStrandServer app = acceptingServer(server)
                .handlerFactory(connection -> {
                    hold.await();
                    return PreparedHandler.of(request -> {});
                })
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);
        NettyStrandClient client = clientFixture.clientBuilder().build();
        NettyStrandConnection first = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        awaitTrue(() -> app.resourceUsage(Resource.FACTORY_TASKS).current() == 1);

        NettyStrandConnection second = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        PreparationFailure failure = failures.poll(15, TimeUnit.SECONDS);

        assertThat(failure).isNotNull();
        assertThat(failure.stage()).isEqualTo(PreparationStage.HANDLER_FACTORY);
        assertThat(failure.cause()).isInstanceOf(ResourceExhaustedException.class);
        hold.countDown();
        first.abort();
        second.abort();
    }

    @Test
    void preparingConnectionSlotsAreCountedSeparatelyFromTotalConnections() throws Exception {
        AppFixture server = track(new AppFixture.Spec().preparing(1).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch hold = new CountDownLatch(1);
        BlockingQueue<PreparationFailure> failures = new LinkedBlockingQueue<>();
        NettyStrandServer app = server.serverBuilder()
                .authenticate(credentials -> {
                    hold.await();
                    return AuthenticationResult.accepted("p");
                })
                .handler(request -> {})
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);
        NettyStrandClient client = clientFixture.clientBuilder().build();
        CompletableFuture<NettyStrandConnection> first = AppFixture.cf(client.connect(AppFixture.loopback(listener)));
        awaitTrue(() -> app.resourceUsage(Resource.PREPARING_CONNECTIONS).current() == 1);

        AppFixture.cf(client.connect(AppFixture.loopback(listener)));
        PreparationFailure failure = failures.poll(15, TimeUnit.SECONDS);

        assertThat(failure).isNotNull();
        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(((ResourceExhaustedException) failure.cause()).resource()).isEqualTo(Resource.PREPARING_CONNECTIONS);
        hold.countDown();
        AppFixture.get(first).abort();
        // 준비가 끝나면 준비 슬롯은 돌아오고 전체 연결 슬롯은 연결이 끝날 때까지 남는다.
        awaitTrue(() -> app.resourceUsage(Resource.PREPARING_CONNECTIONS).current() == 0);
    }

    @Test
    void requiredProgressSpaceIsReservedFromTheOwnerBudgetAndRefusesTheConnectionWhenItIsShort() throws Exception {
        AppFixture server =
                track(new AppFixture.Spec().ownerProgressConnections(1).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        BlockingQueue<PreparationFailure> failures = new LinkedBlockingQueue<>();
        NettyStrandServer app = acceptingServer(server)
                .handler(request -> {})
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);
        NettyStrandClient client = clientFixture.clientBuilder().build();
        NettyStrandConnection first = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        AppFixture.cf(client.connect(AppFixture.loopback(listener)));
        PreparationFailure failure = failures.poll(15, TimeUnit.SECONDS);

        assertThat(failure).isNotNull();
        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(((ResourceExhaustedException) failure.cause()).resource()).isEqualTo(Resource.FRAME_ASSEMBLY_BYTES);
        // 부분 실패는 확보분을 모두 돌려준다: 첫 연결 몫만 남는다.
        assertThat(app.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isEqualTo(1);
        assertThat(app.resourceUsage(Resource.PREPARING_CONNECTIONS).current()).isIn(0L, 1L);
        first.abort();
    }

    @Test
    void theClientRefusesToStartAnAttemptWhenItsOwnConnectionSlotsAreUsed() throws Exception {
        AppFixture fixture = track(new AppFixture.Spec().connections(1).plain());
        NettyStrandServer server =
                acceptingServer(fixture).handler(request -> {}).build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyStrandConnection first = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        CompletableFuture<NettyStrandConnection> second = AppFixture.cf(client.connect(AppFixture.loopback(listener)));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> second.get(15, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(ResourceExhaustedException.class);
        first.abort();
    }

    @Test
    void aHugeRequestDeadlineBehavesLikeNoDeadlineWithoutOverflow() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandServer server = acceptingServer(fixture)
                .handler(request -> request.respond(Status.OK, bytes("ok")))
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));

        RequestResult seconds = fixture.result(connection
                .request(Request.of(TYPE, bytes("a")), RequestOptions.deadlineAfter(Duration.ofSeconds(Long.MAX_VALUE)))
                .result());
        RequestResult nanos = fixture.result(connection
                .request(Request.of(TYPE, bytes("b")), RequestOptions.deadlineAfter(Duration.ofNanos(Long.MAX_VALUE)))
                .result());

        assertThat(seconds.status()).isEqualTo(Status.OK);
        assertThat(nanos.status()).isEqualTo(Status.OK);
    }

    @Test
    void anUnresolvedLoopbackNameIsResolvedInsideTheResolveSlot() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandServer server =
                acceptingServer(fixture).handler(request -> {}).build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();

        NettyStrandConnection connection = AppFixture.get(client.connect(java.net.InetSocketAddress.createUnresolved(
                "localhost", ((java.net.InetSocketAddress) listener.localAddress()).getPort())));

        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).highWater()).isEqualTo(1);
        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isZero();
        connection.abort();
    }

    @Test
    void closingTheListenerStopsNewConnectionsButKeepsTheRegisteredOne() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandServer server = acceptingServer(fixture)
                .handler(request -> request.respond(Status.OK, request.payload()))
                .build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyStrandConnection kept = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        listener.close();

        // 닫기는 EventLoop에서 이어지므로, 새 연결이 실패할 때까지 시도한다. 실패 전에 맺어진 연결은 정리한다.
        awaitTrue(() -> {
            CompletableFuture<NettyStrandConnection> attempt =
                    AppFixture.cf(client.connect(AppFixture.loopback(listener)));
            try {
                attempt.get(15, TimeUnit.SECONDS).abort();
                return false;
            } catch (java.util.concurrent.ExecutionException e) {
                return e.getCause() instanceof PreparationFailedException;
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        RequestResult result = fixture.result(
                kept.request(Request.of(AppFixture.TYPE, new byte[] {1})).result());
        assertThat(result.status()).isEqualTo(Status.OK);
        kept.abort();
    }

    @Test
    void requestsQueuedBehindASlowHandlerAreNotStartedOnceTheConnectionHasEnded() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger started = new AtomicInteger();
        CompletableFuture<NettyStrandConnection> accepted = new CompletableFuture<>();
        NettyStrandServer app = acceptingServer(server)
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> {
                        started.incrementAndGet();
                        firstEntered.countDown();
                        release.await();
                    });
                })
                .build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        clientFixture.requestWhenServed(connection, Request.of(TYPE, bytes("probe")), true);
        connection.request(Request.of(TYPE, bytes("b")));
        connection.request(Request.of(TYPE, bytes("c")));
        awaitTrue(() -> app.resourceUsage(Resource.HANDLER_TASKS).current() == 3);

        connection.abort();
        server.result(accepted.get(15, TimeUnit.SECONDS).closed());
        release.countDown();

        // 직렬 칸에서 기다리던 핸들러는 연결이 끝난 뒤 시작하지 않는다.
        awaitTrue(() -> app.resourceUsage(Resource.HANDLER_TASKS).current() == 0);
        assertThat(started.get()).isEqualTo(1);
    }
}
