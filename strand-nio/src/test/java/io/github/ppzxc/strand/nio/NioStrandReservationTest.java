package io.github.ppzxc.strand.nio;

import static io.github.ppzxc.strand.nio.AppFixture.TYPE;
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

class NioStrandReservationTest {

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

    private static NioStrandServer.Builder acceptingServer(AppFixture fixture) {
        return fixture.serverBuilder().authenticate(c -> AuthenticationResult.accepted("p"));
    }

    @Test
    void aFirstRequestSentRightAfterConnectingIsHeldUntilTheHandlerFactoryFinishes() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch factoryStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        NioStrandServer app = acceptingServer(server)
                .handlerFactory(connection -> {
                    factoryStarted.countDown();
                    release.await();
                    return PreparedHandler.of(request -> request.respond(Status.OK, new byte[0]));
                })
                .build();
        Listener listener = listen(app);
        NioStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        assertThat(factoryStarted.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                .isTrue();

        RequestHandle handle = connection.request(Request.of(TYPE, bytes("a")));
        Thread.sleep(200);
        assertThat(handle.result().isDone()).isFalse();
        release.countDown();

        assertThat(clientFixture.result(handle.result()).status()).isEqualTo(Status.OK);
    }

    @Test
    void aRequestHeldDuringPreparationIsRefusedAsNotProcessedWhenTheFactoryFails() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch release = new CountDownLatch(1);
        NioStrandServer app = acceptingServer(server)
                .handlerFactory(connection -> {
                    release.await();
                    throw new IllegalStateException("팩터리 실패");
                })
                .build();
        Listener listener = listen(app);
        NioStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));

        RequestHandle handle = connection.request(Request.of(TYPE, bytes("a")));
        Thread.sleep(200);
        release.countDown();
        RequestResult result = clientFixture.result(handle.result());

        assertThat(result.status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(result.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
    }

    @Test
    void receivedRequestsBeyondTheOwnerLimitAreRefusedBeforeAnyHandlerStarts() throws Exception {
        AppFixture server =
                track(new AppFixture.Spec().ownerIncoming(2, 1 << 20).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        AtomicInteger started = new AtomicInteger();
        NioStrandServer app = acceptingServer(server)
                .handler(request -> started.incrementAndGet())
                .build();
        Listener listener = listen(app);
        NioStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        connection.request(Request.of(TYPE, bytes("a")));
        connection.request(Request.of(TYPE, bytes("b")));
        awaitTrue(() -> started.get() == 2);

        RequestResult refused = clientFixture.result(
                connection.request(Request.of(TYPE, bytes("c"))).result());

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
        NioStrandServer app = acceptingServer(server)
                .handler(request -> request.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = listen(app);
        NioStrandConnection connection =
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
        NioStrandServer app = acceptingServer(server)
                .handler(request -> {
                    started.incrementAndGet();
                    hold.await();
                    request.respond(Status.OK, new byte[0]);
                })
                .build();
        Listener listener = listen(app);
        NioStrandConnection connection =
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
        NioStrandServer app = acceptingServer(server)
                .handler(request -> started.incrementAndGet())
                .build();
        Listener listener = listen(app);
        NioStrandConnection connection =
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
        NioStrandServer server = acceptingServer(fixture).handler(r -> {}).build();
        Listener listener = listen(server);
        NioStrandClient client = fixture.clientBuilder().build();
        NioStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));

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
        NioStrandServer server =
                acceptingServer(fixture).maxFrameSize(16384).handler(r -> {}).build();
        Listener listener = listen(server);
        NioStrandClient client = fixture.clientBuilder().build();
        NioStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));

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
        NioStrandServer app = acceptingServer(server).handler(seen::complete).build();
        Listener listener = listen(app);
        NioStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("q")));
        IncomingRequest request = seen.get(15, TimeUnit.SECONDS);

        ResponseAcceptance tooBig = server.result(request.respond(Status.OK, bytes("12345")));
        ResponseAcceptance retried = server.result(request.respond(Status.OK, bytes("1234")));
        ResponseAcceptance again = server.result(request.respond(Status.OK, bytes("12")));

        assertThat(tooBig.kind()).isEqualTo(AcceptanceKind.RESOURCE_EXHAUSTED);
        assertThat(tooBig.resource()).hasValue(Resource.RESPONSE_BYTES);

        assertThat(retried.kind()).isEqualTo(AcceptanceKind.ACCEPTED);
        assertThat(again.kind()).isEqualTo(AcceptanceKind.ALREADY_RESPONDED);
        assertThat(clientFixture.result(handle.result()).payload()).isEqualTo(bytes("1234"));

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
        NioStrandServer app = acceptingServer(server).handler(seen::complete).build();
        Listener listener = listen(app);
        NioStrandConnection connection = AppFixture.get(
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
        NioStrandServer app = acceptingServer(server).handler(seen::add).build();
        Listener listener = listen(app);
        NioStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle first = connection.request(Request.of(TYPE, bytes("first")));
        IncomingRequest cancelled = seen.poll(15, TimeUnit.SECONDS);
        first.cancel();

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
        NioStrandServer server =
                acceptingServer(fixture).handler(seen::complete).build();
        Listener listener = listen(server);
        NioStrandConnection connection =
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
        NioStrandServer server = acceptingServer(fixture).handler(request -> {}).build();
        Listener listener = listen(server);
        NioStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("slow")));
        CountDownLatch release = new CountDownLatch(1);
        Observation first = handle.result().observe(result -> await(release));
        Observation second = handle.result().observe(result -> {});

        assertThat(second.isDone()).isTrue();
        assertThat(second.failureNow()).get().isInstanceOf(ResourceExhaustedException.class);
        handle.cancel();

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
        CompletableFuture<NioStrandConnection> accepted = new CompletableFuture<>();
        NioStrandServer server = acceptingServer(fixture)
                .handlerFactory(connection -> {
                    accepted.complete(connection);
                    return PreparedHandler.of(request -> request.respond(Status.OK, new byte[0]));
                })
                .build();
        Listener listener = listen(server);
        NioStrandConnection connection = AppFixture.get(fixture.clientBuilder()
                .handler(request -> {
                    handlerEntered.countDown();
                    release.await();
                    request.respond(Status.OK, new byte[0]);
                })
                .build()
                .connect(AppFixture.loopback(listener)));

        accepted.get(15, TimeUnit.SECONDS).request(Request.of(TYPE, bytes("hold")));
        assertThat(handlerEntered.await(15, TimeUnit.SECONDS)).isTrue();
        AtomicInteger observed = new AtomicInteger();
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("x")));
        handle.result().observe(result -> observed.incrementAndGet());

        awaitTrue(() -> handle.result().isDone());

        assertThat(handle.result().resultNow().status()).isEqualTo(Status.OK);
        assertThat(observed.get()).isZero();
        release.countDown();
        awaitTrue(() -> observed.get() == 1);
    }

    @Test
    void whenTheAppExecutorRejectsEverythingAfterConnectResultsAreRetainedAndOnlyObservationsFail() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        NioStrandServer app = acceptingServer(server)
                .handler(request -> request.respond(Status.OK, new byte[0]))
                .build();
        Listener listener = listen(app);
        NioStrandClient client = clientFixture.clientBuilder().build();

        NioStrandConnection connection = AppFixture.get(client.connect(AppFixture.loopback(listener)));

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
        NioStrandServer app = acceptingServer(server)
                .handler(request -> {})
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);

        server.executor.rejectTaskNumber(2);

        CompletableFuture<NioStrandConnection> stage = clientFixture
                .clientBuilder()
                .build()
                .connect(AppFixture.loopback(listener))
                .toCompletableFuture();

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
        NioStrandServer app = server.serverBuilder()
                .authenticate(c -> {
                    server.executor.rejectNextTask();
                    return AuthenticationResult.accepted("p");
                })
                .handlerFactory(connection -> PreparedHandler.of(request -> {}))
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);

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
        NioStrandServer app = server.serverBuilder()
                .authenticate(credentials -> {
                    hold.await();
                    return AuthenticationResult.accepted("p");
                })
                .handler(request -> {})
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);
        NioStrandClient client = clientFixture.clientBuilder().build();
        CompletableFuture<NioStrandConnection> first =
                client.connect(AppFixture.loopback(listener)).toCompletableFuture();
        awaitTrue(() -> app.resourceUsage(Resource.AUTHENTICATION_TASKS).current() == 1);

        CompletableFuture<NioStrandConnection> second =
                client.connect(AppFixture.loopback(listener)).toCompletableFuture();
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
        NioStrandServer app = acceptingServer(server)
                .handlerFactory(connection -> {
                    hold.await();
                    return PreparedHandler.of(request -> {});
                })
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);
        NioStrandClient client = clientFixture.clientBuilder().build();
        NioStrandConnection first = AppFixture.get(client.connect(AppFixture.loopback(listener)));
        awaitTrue(() -> app.resourceUsage(Resource.FACTORY_TASKS).current() == 1);

        NioStrandConnection second = AppFixture.get(client.connect(AppFixture.loopback(listener)));
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
        NioStrandServer app = server.serverBuilder()
                .authenticate(credentials -> {
                    hold.await();
                    return AuthenticationResult.accepted("p");
                })
                .handler(request -> {})
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);
        NioStrandClient client = clientFixture.clientBuilder().build();
        CompletableFuture<NioStrandConnection> first =
                client.connect(AppFixture.loopback(listener)).toCompletableFuture();
        awaitTrue(() -> app.resourceUsage(Resource.PREPARING_CONNECTIONS).current() == 1);

        client.connect(AppFixture.loopback(listener)).toCompletableFuture();
        PreparationFailure failure = failures.poll(15, TimeUnit.SECONDS);

        assertThat(failure).isNotNull();
        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(((ResourceExhaustedException) failure.cause()).resource()).isEqualTo(Resource.PREPARING_CONNECTIONS);
        hold.countDown();
        AppFixture.get(first).abort();

        awaitTrue(() -> app.resourceUsage(Resource.PREPARING_CONNECTIONS).current() == 0);
    }

    @Test
    void requiredProgressSpaceIsReservedFromTheOwnerBudgetAndRefusesTheConnectionWhenItIsShort() throws Exception {
        AppFixture server =
                track(new AppFixture.Spec().ownerProgressConnections(1).plain());
        AppFixture clientFixture = track(AppFixture.plain());
        BlockingQueue<PreparationFailure> failures = new LinkedBlockingQueue<>();
        NioStrandServer app = acceptingServer(server)
                .handler(request -> {})
                .onConnectionFailure(failures::add)
                .build();
        Listener listener = listen(app);
        NioStrandClient client = clientFixture.clientBuilder().build();
        NioStrandConnection first = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        client.connect(AppFixture.loopback(listener)).toCompletableFuture();
        PreparationFailure failure = failures.poll(15, TimeUnit.SECONDS);

        assertThat(failure).isNotNull();
        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(((ResourceExhaustedException) failure.cause()).resource()).isEqualTo(Resource.FRAME_ASSEMBLY_BYTES);

        assertThat(app.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isEqualTo(1);
        assertThat(app.resourceUsage(Resource.PREPARING_CONNECTIONS).current()).isIn(0L, 1L);
        first.abort();
    }

    @Test
    void theClientRefusesToStartAnAttemptWhenItsOwnConnectionSlotsAreUsed() throws Exception {
        AppFixture fixture = track(new AppFixture.Spec().connections(1).plain());
        NioStrandServer server = acceptingServer(fixture).handler(request -> {}).build();
        Listener listener = listen(server);
        NioStrandClient client = fixture.clientBuilder().build();
        NioStrandConnection first = AppFixture.get(client.connect(AppFixture.loopback(listener)));

        CompletableFuture<NioStrandConnection> second =
                client.connect(AppFixture.loopback(listener)).toCompletableFuture();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> second.get(15, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(ResourceExhaustedException.class);
        first.abort();
    }

    @Test
    void aHugeRequestDeadlineBehavesLikeNoDeadlineWithoutOverflow() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NioStrandServer server = acceptingServer(fixture)
                .handler(request -> request.respond(Status.OK, bytes("ok")))
                .build();
        Listener listener = listen(server);
        NioStrandConnection connection =
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
        NioStrandServer server = acceptingServer(fixture).handler(request -> {}).build();
        Listener listener = listen(server);
        NioStrandClient client = fixture.clientBuilder().build();

        NioStrandConnection connection = AppFixture.get(client.connect(java.net.InetSocketAddress.createUnresolved(
                "localhost", ((java.net.InetSocketAddress) listener.localAddress()).getPort())));

        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).highWater()).isEqualTo(1);
        assertThat(client.resourceUsage(Resource.RESOLVE_TASKS).current()).isZero();
        connection.abort();
    }

    @Test
    void anAcceptedSocketThatRacesTheListenerCloseIsClosedNotRegistered() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NioStrandServer server = acceptingServer(fixture).handler(request -> {}).build();
        Listener listener = listen(server);
        java.nio.channels.ServerSocketChannel helper = java.nio.channels.ServerSocketChannel.open();
        closeables.add(helper::close);
        helper.bind(AppFixture.anyLoopback());
        java.nio.channels.SocketChannel client = java.nio.channels.SocketChannel.open(helper.getLocalAddress());
        closeables.add(client::close);
        java.nio.channels.SocketChannel accepted = helper.accept();
        AtomicInteger registered = new AtomicInteger();
        listener.close();

        listener.handleAccepted(accepted, channel -> registered.incrementAndGet());

        assertThat(registered.get()).isZero();
        assertThat(accepted.isOpen()).isFalse();
    }

    @Test
    void requestsQueuedBehindASlowHandlerAreNotStartedOnceTheConnectionHasEnded() throws Exception {
        AppFixture server = track(AppFixture.plain());
        AppFixture clientFixture = track(AppFixture.plain());
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger started = new AtomicInteger();
        CompletableFuture<NioStrandConnection> accepted = new CompletableFuture<>();
        NioStrandServer app = acceptingServer(server)
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
        NioStrandConnection connection =
                AppFixture.get(clientFixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        clientFixture.requestWhenServed(connection, Request.of(TYPE, bytes("probe")), true);
        connection.request(Request.of(TYPE, bytes("b")));
        connection.request(Request.of(TYPE, bytes("c")));
        awaitTrue(() -> app.resourceUsage(Resource.HANDLER_TASKS).current() == 3);

        connection.abort();
        server.result(accepted.get(15, TimeUnit.SECONDS).closed());
        release.countDown();

        awaitTrue(() -> app.resourceUsage(Resource.HANDLER_TASKS).current() == 0);
        assertThat(started.get()).isEqualTo(1);
    }
}
