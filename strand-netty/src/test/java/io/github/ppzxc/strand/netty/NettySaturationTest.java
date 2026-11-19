package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.AcceptanceKind;
import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Observation;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.Status;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 연결별·전체 한도를 함께 강제하고, 부분 접수 실패의 예약 반환·자기 포화로 연결을 끝내지 않음·포화 중 취소와 종료의 진행·핸들러 예외 처리·한도 집계를
 * 앱 공개 API로 본다(#324).
 */
class NettySaturationTest {
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

    private static NettyStrandServer.Builder acceptingServer(AppFixture fixture) {
        return fixture.serverBuilder().authenticate(c -> AuthenticationResult.accepted("p"));
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
            Thread.sleep(5);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --- 연결별·전체 한도를 함께 강제하고 부분 실패는 확보분을 돌려준다 ---

    @Test
    void thePerConnectionRequestCountIsEnforcedAndCountedWhileTheOwnerHasRoomToSpare() throws Exception {
        AppFixture fixture = track(new AppFixture.Spec().perConnection(2).plain());
        NettyStrandServer server = acceptingServer(fixture).handler(r -> {}).build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        connection.request(Request.of(TYPE, bytes("a")));
        connection.request(Request.of(TYPE, bytes("b")));
        RequestResult third =
                fixture.result(connection.request(Request.of(TYPE, bytes("c"))).result());

        assertThat(third.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(third.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(third.resource()).hasValue(Resource.OUTGOING_REQUESTS);
        ResourceUsage perConnection = connection.resourceUsage(Resource.OUTGOING_REQUESTS);
        assertThat(perConnection.limit()).isEqualTo(2);
        assertThat(perConnection.current()).isEqualTo(2);
        assertThat(perConnection.highWater()).isEqualTo(2);
        assertThat(perConnection.rejections()).isEqualTo(1);
        // 연결 범위에서 거절돼 소유 객체 범위의 예약과 거절 집계는 건드리지 않았다.
        assertThat(client.resourceUsage(Resource.OUTGOING_REQUESTS).current()).isEqualTo(2);
        assertThat(client.resourceUsage(Resource.OUTGOING_REQUESTS).rejections())
                .isZero();
    }

    @Test
    void thePerConnectionBytesAreEnforcedAndTheCountReservationTakenFirstIsReturned() throws Exception {
        AppFixture fixture = track(new AppFixture.Spec().bodyBytes(10).plain());
        NettyStrandServer server = acceptingServer(fixture).handler(r -> {}).build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        RequestResult rejected = fixture.result(
                connection.request(Request.of(TYPE, new byte[11])).result());

        assertThat(rejected.status()).isEqualTo(Status.RESOURCE_EXHAUSTED);
        assertThat(rejected.resource()).hasValue(Resource.OUTGOING_BYTES);
        assertThat(connection.resourceUsage(Resource.OUTGOING_REQUESTS).current())
                .isZero();
        assertThat(connection.resourceUsage(Resource.OUTGOING_BYTES).current()).isZero();
        assertThat(connection.resourceUsage(Resource.OUTGOING_BYTES).rejections())
                .isEqualTo(1);
        assertThat(client.resourceUsage(Resource.OUTGOING_REQUESTS).current()).isZero();
    }

    @Test
    void anOwnerLimitTighterThanTheConnectionLimitRefusesAndTheConnectionShareTakenFirstIsReturned() throws Exception {
        AppFixture.Spec spec = new AppFixture.Spec().connections(2).perConnection(4);
        spec.owner = 1;
        AppFixture fixture = track(spec.plain());
        NettyStrandServer server = acceptingServer(fixture).handler(r -> {}).build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyStrandConnection first = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        NettyStrandConnection second = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        first.request(Request.of(TYPE, bytes("a")));
        second.request(Request.of(TYPE, bytes("b")));
        RequestResult refused =
                fixture.result(first.request(Request.of(TYPE, bytes("c"))).result());

        assertThat(refused.resource()).hasValue(Resource.OUTGOING_REQUESTS);
        assertThat(client.resourceUsage(Resource.OUTGOING_REQUESTS).limit()).isEqualTo(2);
        assertThat(client.resourceUsage(Resource.OUTGOING_REQUESTS).current()).isEqualTo(2);
        assertThat(client.resourceUsage(Resource.OUTGOING_REQUESTS).rejections())
                .isEqualTo(1);
        // 연결 몫은 소유 객체 거절 때 돌려받아 그 연결에는 요청 하나만 남아 있다.
        assertThat(first.resourceUsage(Resource.OUTGOING_REQUESTS).current()).isEqualTo(1);
    }

    @Test
    void aPerConnectionHandlerSlotShortageRefusesTheRequestAndReturnsTheCountAndBytesTakenFirst() throws Exception {
        AppFixture server =
                track(new AppFixture.Spec().connectionHandlerTasks(1).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch hold = new CountDownLatch(1);
        CompletableFuture<NettyStrandConnection> serverSide = new CompletableFuture<>();
        NettyStrandServer app = acceptingServer(server)
                .handler(request -> {
                    serverSide.complete(request.connection());
                    hold.await();
                })
                .build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        connection.request(Request.of(TYPE, bytes("first")));
        NettyStrandConnection accepted = serverSide.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);

        RequestResult refused = clientFixture.result(
                connection.request(Request.of(TYPE, bytes("second"))).result());
        hold.countDown();

        assertThat(refused.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(refused.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(accepted.resourceUsage(Resource.HANDLER_TASKS).rejections()).isEqualTo(1);
        assertThat(accepted.resourceUsage(Resource.HANDLER_TASKS).highWater()).isEqualTo(1);
        assertThat(app.resourceUsage(Resource.HANDLER_TASKS).rejections()).isZero();
        // 거절된 요청이 먼저 잡은 수·바이트 예약은 돌아왔고 첫 요청의 몫만 남는다.
        assertThat(app.resourceUsage(Resource.INCOMING_REQUESTS).current()).isEqualTo(1);
        assertThat(app.resourceUsage(Resource.INCOMING_BYTES).current()).isEqualTo(bytes("first").length);
    }

    // --- 자기 포화만으로 연결을 끝내지 않는다 ---

    @Test
    void refusingManyRequestsForSelfSaturationNeverEndsTheConnectionAndLaterRequestsStillWork() throws Exception {
        AppFixture server =
                track(new AppFixture.Spec().ownerIncoming(1, 1 << 20).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch hold = new CountDownLatch(1);
        BlockingQueue<IncomingRequest> seen = new LinkedBlockingQueue<>();
        NettyStrandServer app = acceptingServer(server)
                .handler(request -> {
                    seen.add(request);
                    if (request.payload()[0] == 'x') {
                        hold.await();
                    } else {
                        request.respond(Status.OK, bytes("ok"));
                    }
                })
                .build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle occupying = connection.request(Request.of(TYPE, bytes("x")));
        assertThat(seen.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)).isNotNull();

        for (int i = 0; i < 5; i++) {
            RequestResult refused = clientFixture.result(
                    connection.request(Request.of(TYPE, bytes("y"))).result());
            assertThat(refused.status()).isEqualTo(Status.UNAVAILABLE);
            assertThat(refused.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        }
        assertThat(connection.closed().isDone()).isFalse();
        hold.countDown();
        // 점유하던 요청이 끝나면 같은 연결에서 요청이 다시 처리된다.
        IncomingRequest held = seen.poll(1, TimeUnit.SECONDS);
        assertThat(held).isNull();
        occupying.cancel();
        awaitTrue(() -> app.resourceUsage(Resource.INCOMING_REQUESTS).current() == 0);
        RequestResult ok = clientFixture.result(
                connection.request(Request.of(TYPE, bytes("z"))).result());

        assertThat(ok.status()).isEqualTo(Status.OK);
        assertThat(app.resourceUsage(Resource.INCOMING_REQUESTS).rejections()).isEqualTo(5);
        assertThat(connection.closed().isDone()).isFalse();
    }

    // --- 응답 부족과 재시도 ---

    @Test
    void aRetryAfterAResourceShortageIsJudgedAsEndedBeforeAnyShortage() throws Exception {
        AppFixture server = track(new AppFixture.Spec().ownerResponseBytes(4).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CompletableFuture<IncomingRequest> seen = new CompletableFuture<>();
        NettyStrandServer app = acceptingServer(server).handler(seen::complete).build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("q")));
        IncomingRequest request = seen.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);

        ResponseAcceptance shortage = server.result(request.respond(Status.OK, bytes("12345")));
        handle.cancel();
        awaitTrue(() -> request.ended().isDone());
        ResponseAcceptance retried = server.result(request.respond(Status.OK, bytes("12345")));

        assertThat(shortage.kind()).isEqualTo(AcceptanceKind.RESOURCE_EXHAUSTED);
        assertThat(shortage.resource()).hasValue(Resource.RESPONSE_BYTES);
        // 다시 시도했을 때는 요청이 끝나 있으므로 부족이 아니라 끝남으로 판정한다.
        assertThat(retried.kind()).isEqualTo(AcceptanceKind.REQUEST_ENDED);
        assertThat(app.resourceUsage(Resource.RESPONSE_BYTES).current()).isZero();
        assertThat(app.resourceUsage(Resource.RESPONSE_BYTES).rejections()).isEqualTo(1);
    }

    @Test
    void onlyOneResponseCommandWaitsPerRequestEvenWhenTheBodyIsEmptyAndReservesNothing() throws Exception {
        BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
        NettyServerRig rig = new NettyServerRig(received::add);
        try {
            rig.peerRequest(new byte[] {1});
            IncomingRequest request = received.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(request).isNotNull();
            rig.closeGate();

            Result<ResponseAcceptance> first = request.respond(Status.OK, new byte[0]);
            Result<ResponseAcceptance> second = request.respond(Status.OK, new byte[0]);

            // 이벤트 루프가 멈춘 동안 첫 응답만 접수 대기를 점유하고, 두 번째는 기다리지 않고 부족으로 끝난다.
            assertThat(first.isDone()).isFalse();
            assertThat(second.isDone()).isTrue();
            assertThat(second.resultNow().kind()).isEqualTo(AcceptanceKind.RESOURCE_EXHAUSTED);
            assertThat(second.resultNow().resource()).hasValue(Resource.RESPONSE_BYTES);
            rig.openGate();
            awaitTrue(first::isDone);
            assertThat(first.resultNow().kind()).isEqualTo(AcceptanceKind.ACCEPTED);
            assertThat(request.respond(Status.OK, new byte[0]).resultNow().kind())
                    .isEqualTo(AcceptanceKind.ALREADY_RESPONDED);
        } finally {
            rig.close();
        }
    }

    // --- 핸들러 예외 ---

    private static final class CapturingHandler extends Handler {
        final List<LogRecord> records = new CopyOnWriteArrayList<>();

        @Override
        public void publish(LogRecord record) {
            records.add(record);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }

    @Test
    void aHandlerExceptionAnswersWithAnEmptyInternalResponseKeepsTheConnectionAndIsOnlyLoggedLocally()
            throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NettyStrandServer app = acceptingServer(server)
                .handler(request -> {
                    if (request.payload()[0] == 'b') {
                        request.respond(Status.OK, bytes("answered"));
                    }
                    if (request.payload()[0] != 'g') {
                        throw new IllegalStateException("secret-detail");
                    }
                    request.respond(Status.OK, bytes("fine"));
                })
                .build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        CapturingHandler capture = new CapturingHandler();
        Logger logger = Logger.getLogger("io.github.ppzxc.strand.netty");
        logger.addHandler(capture);
        try {
            RequestResult failed = clientFixture.result(
                    connection.request(Request.of(TYPE, bytes("a"))).result());
            RequestResult alreadyAnswered = clientFixture.result(
                    connection.request(Request.of(TYPE, bytes("b"))).result());
            RequestResult fine = clientFixture.result(
                    connection.request(Request.of(TYPE, bytes("g"))).result());

            assertThat(failed.status()).isEqualTo(Status.INTERNAL);
            assertThat(failed.certainty()).isEqualTo(Certainty.RESPONSE);
            assertThat(failed.payload()).isEmpty();
            // 이미 응답한 요청은 예외가 나도 추가로 응답하지 않는다.
            assertThat(alreadyAnswered.status()).isEqualTo(Status.OK);
            assertThat(alreadyAnswered.payload()).isEqualTo(bytes("answered"));
            assertThat(fine.status()).isEqualTo(Status.OK);
            assertThat(connection.closed().isDone()).isFalse();
            awaitTrue(() -> capture.records.size() >= 2);
            assertThat(capture.records).allSatisfy(record -> {
                assertThat(record.getLevel()).isEqualTo(Level.WARNING);
                assertThat(record.getThrown()).hasMessage("secret-detail");
            });
            awaitTrue(() -> app.resourceUsage(Resource.HANDLER_TASKS).current() == 0);
        } finally {
            logger.removeHandler(capture);
        }
    }

    // --- 포화 중에도 취소·종료·결과 보존이 진행된다 ---

    @Test
    void cancelAndCloseProgressWhileRequestCountAndObservationLimitsAreFullAndTheDefaultCloseNoticeStillFits()
            throws Exception {
        AppFixture fixture =
                track(new AppFixture.Spec().perConnection(2).observations(1, 64).plain());
        NettyStrandServer server = acceptingServer(fixture).handler(r -> {}).build();
        Listener listener = listen(server);
        NettyStrandClient client = fixture.clientBuilder().build();
        NettyStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        RequestHandle first = connection.request(Request.of(TYPE, bytes("a")));
        RequestHandle second = connection.request(Request.of(TYPE, bytes("b")));
        CountDownLatch release = new CountDownLatch(1);
        Observation occupying = first.result().observe(fixture.pool, result -> await(release));
        // 관찰 한도가 차 있으므로 결과는 관찰 없이 조회한다.
        RequestResult countFull =
                connection.request(Request.of(TYPE, bytes("c"))).result().resultNow();
        Observation general = second.result().observe(result -> {});
        CompletableFuture<CloseNotice> notice = new CompletableFuture<>();
        Observation closeNotice = connection.closed().observe(notice::complete);
        Observation secondCloseObserver = connection.closed().observe(n -> {});

        assertThat(countFull.resource()).hasValue(Resource.OUTGOING_REQUESTS);
        assertThat(general.failureNow()).get().isInstanceOf(ResourceExhaustedException.class);
        // 일반 관찰이 포화여도 기본 종료 알림 한 건은 별도 자리로 등록된다. 그 자리가 쓰이는 동안 오는 다른 관찰은 일반 한도를 쓰므로 거절된다.
        assertThat(closeNotice.isDone()).isFalse();
        assertThat(secondCloseObserver.failureNow()).get().isInstanceOf(ResourceExhaustedException.class);
        assertThat(connection.resourceUsage(Resource.OBSERVATIONS).current()).isEqualTo(1);

        first.cancel();
        second.cancel();
        awaitTrue(() -> first.result().isDone() && second.result().isDone());
        connection.close();
        CloseNotice closed = notice.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);

        assertThat(first.result().resultNow().status()).isEqualTo(Status.CANCELLED);
        assertThat(second.result().resultNow().status()).isEqualTo(Status.CANCELLED);
        assertThat(closed.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(connection.resourceUsage(Resource.OUTGOING_REQUESTS).current())
                .isZero();
        assertThat(connection.resourceUsage(Resource.OUTGOING_REQUESTS).rejections())
                .isEqualTo(1);
        assertThat(occupying.isDone()).isFalse();
        release.countDown();
        awaitTrue(occupying::isDone);
        awaitTrue(() -> connection.resourceUsage(Resource.OBSERVATIONS).current() == 0);
    }

    @Test
    void abortProgressesWhileTheHandlerSlotsAreHeldByHandlersThatNeverReturn() throws Exception {
        AppFixture server =
                track(new AppFixture.Spec().connectionHandlerTasks(1).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch hold = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        NettyStrandServer app = acceptingServer(server)
                .handler(request -> {
                    started.countDown();
                    await(hold);
                })
                .build();
        Listener listener = listen(app);
        NettyStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle pending = connection.request(Request.of(TYPE, bytes("first")));
        assertThat(started.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

        connection.abort();
        Object notice = clientFixture.result(connection.closed());

        // 핸들러가 반환하지 않아 슬롯을 점유해도 결과 확정·종료 알림은 진행된다.
        assertThat(notice).isNotNull();
        assertThat(pending.result().isDone()).isTrue();
        assertThat(app.resourceUsage(Resource.HANDLER_TASKS).current()).isEqualTo(1);
        hold.countDown();
        awaitTrue(() -> app.resourceUsage(Resource.HANDLER_TASKS).current() == 0);
        awaitTrue(() -> app.resourceUsage(Resource.INCOMING_BYTES).current() == 0);
    }

    // --- 관찰 슬롯은 실행기가 작업을 실행할 때까지 유지된다 ---

    private static final class HeldExecutor implements Executor {
        private final Queue<Runnable> held = new ArrayDeque<>();

        @Override
        public synchronized void execute(Runnable task) {
            held.add(task);
        }

        synchronized void runAll() {
            Runnable task;
            while ((task = held.poll()) != null) {
                task.run();
            }
        }
    }

    @Test
    void anObservationSlotIsKeptUntilAnExecutorThatDoesNotRunItsTasksFinallyRunsThemAndTheResultStaysReadable()
            throws Exception {
        AppFixture fixture = track(new AppFixture.Spec().observations(1, 64).plain());
        NettyStrandServer server = acceptingServer(fixture)
                .handler(request -> request.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = listen(server);
        NettyStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("q")));
        awaitTrue(() -> handle.result().isDone());
        HeldExecutor held = new HeldExecutor();

        Observation observation = handle.result().observe(held, result -> {});
        Observation rejected = handle.result().observe(result -> {});

        assertThat(observation.isDone()).isFalse();
        assertThat(connection.resourceUsage(Resource.OBSERVATIONS).current()).isEqualTo(1);
        assertThat(rejected.failureNow()).get().isInstanceOf(ResourceExhaustedException.class);
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.OK);
        held.runAll();
        assertThat(observation.isDone()).isTrue();
        awaitTrue(() -> connection.resourceUsage(Resource.OBSERVATIONS).current() == 0);
        assertThat(connection.resourceUsage(Resource.OBSERVATIONS).highWater()).isEqualTo(1);
        assertThat(connection.resourceUsage(Resource.OBSERVATIONS).rejections()).isEqualTo(1);
    }
}
