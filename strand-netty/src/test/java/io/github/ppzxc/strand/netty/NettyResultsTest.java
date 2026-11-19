package io.github.ppzxc.strand.netty;

import static io.github.ppzxc.strand.netty.AppFixture.TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ResultCompletion;
import io.github.ppzxc.strand.api.Status;
import io.netty.util.concurrent.Future;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 요청 결과를 연결의 EventLoop에 연결된 Netty Future로 바꾸는 변환과 Future 취소의 독립성을 실제 TCP 연결로 검증한다(#327). */
class NettyResultsTest {
    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void cleanUp() throws Exception {
        for (AutoCloseable closeable : closeables) {
            closeable.close();
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private AppFixture track(AppFixture fixture) {
        closeables.add(fixture);
        return fixture;
    }

    /** 서버 핸들러로 연결된 클라이언트 연결. */
    private NettyStrandConnection connect(AppFixture fixture, RequestHandler handler) throws Exception {
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(handler)
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        closeables.add(listener::close);
        NettyStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        closeables.add(connection::abort);
        return connection;
    }

    private static void awaitTrue(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("조건이 충족되지 않았다");
            Thread.sleep(5);
        }
    }

    @Test
    void aResponseIsTheFutureSuccessValueOnTheConnectionEventLoop() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandConnection connection = connect(
                fixture,
                request -> request.respond(
                        Status.OK, bytes("echo:" + new String(request.payload(), StandardCharsets.UTF_8))));

        Future<RequestResult> future = NettyResults.toFuture(
                connection.request(Request.of(TYPE, bytes("hi"))).result());
        CompletableFuture<String> listenerThread = new CompletableFuture<>();
        future.addListener(
                done -> listenerThread.complete(Thread.currentThread().getName()));

        RequestResult result = AppFixture.get(AppFixture.cf(future));
        assertThat(result.status()).isEqualTo(Status.OK);
        assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(new String(result.payload(), StandardCharsets.UTF_8)).isEqualTo("echo:hi");
        assertThat(listenerThread.get(15, TimeUnit.SECONDS)).startsWith("nioEventLoopGroup");
    }

    @Test
    void errorResponseCancelAndDisconnectAreSuccessValuesNotFutureFailures() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandConnection connection = connect(fixture, request -> {
            if (new String(request.payload(), StandardCharsets.UTF_8).equals("err")) {
                request.respond(Status.INTERNAL, bytes("bad"));
            }
        });

        Future<RequestResult> error = NettyResults.toFuture(
                connection.request(Request.of(TYPE, bytes("err"))).result());
        RequestHandle slow = connection.request(Request.of(TYPE, bytes("slow")));
        Future<RequestResult> cancelled = NettyResults.toFuture(slow.result());
        slow.cancel();
        assertThat(AppFixture.get(AppFixture.cf(error)).status()).isEqualTo(Status.INTERNAL);
        assertThat(AppFixture.get(AppFixture.cf(cancelled)).status()).isEqualTo(Status.CANCELLED);
        RequestHandle pending = connection.request(Request.of(TYPE, bytes("pending")));
        Future<RequestResult> disconnected = NettyResults.toFuture(pending.result());
        connection.abort();

        RequestResult end = AppFixture.get(AppFixture.cf(disconnected));
        assertThat(disconnected.isSuccess()).isTrue();
        assertThat(end.certainty()).isNotNull();
        // 접수 실패(연결이 끝난 뒤)도 성공 값이다.
        Future<RequestResult> rejected = NettyResults.toFuture(
                connection.request(Request.of(TYPE, bytes("late"))).result());
        assertThat(AppFixture.get(AppFixture.cf(rejected)).status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(rejected.isSuccess()).isTrue();
    }

    @Test
    void completionDoesNotWaitForAppExecutorOrAppObservers() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        CompletableFuture<IncomingRequest> seen = new CompletableFuture<>();
        NettyStrandConnection connection = connect(fixture, seen::complete);
        CountDownLatch release = new CountDownLatch(1);
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("x")));
        // 앱 observe가 앱 실행기 스레드를 붙잡고, 이후 실행기는 새 작업을 거절한다.
        handle.result().observe(result -> await(release));
        IncomingRequest request = seen.get(15, TimeUnit.SECONDS);
        Future<RequestResult> future = NettyResults.toFuture(handle.result());
        fixture.executor.rejecting = true;

        request.respond(Status.OK, bytes("ok"));

        awaitTrue(future::isDone);
        assertThat(future.isSuccess()).isTrue();
        assertThat(future.getNow().status()).isEqualTo(Status.OK);
        release.countDown();
    }

    @Test
    void conversionsAreIndependentAndCancellingOneAffectsNeitherTheOthersNorTheRequest() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        CountDownLatch answer = new CountDownLatch(1);
        List<IncomingRequest> seen = new CopyOnWriteArrayList<>();
        NettyStrandConnection connection = connect(fixture, request -> {
            seen.add(request);
            answer.countDown();
        });
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("x")));
        Future<RequestResult> a = NettyResults.toFuture(handle.result());
        Future<RequestResult> b = NettyResults.toFuture(handle.result());
        Future<RequestResult> c = NettyResults.toFuture(handle.result());

        assertThat(a).isNotSameAs(b);
        assertThat(b.cancel(false)).isTrue();
        assertThat(b.isCancelled()).isTrue();
        assertThat(answer.await(15, TimeUnit.SECONDS)).isTrue();
        // Future 취소는 실제 요청을 취소하지 않는다: 상대는 여전히 요청 중이고 결과도 미확정이다.
        assertThat(handle.result().isDone()).isFalse();
        assertThat(seen.get(0).ended().isDone()).isFalse();
        seen.get(0).respond(Status.OK, bytes("late"));

        RequestResult first = AppFixture.get(AppFixture.cf(a));
        RequestResult third = AppFixture.get(AppFixture.cf(c));
        assertThat(first.status()).isEqualTo(Status.OK);
        assertThat(third.payload()).isEqualTo(first.payload());
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.OK);
        assertThat(b.isCancelled()).isTrue();
        assertThat(NettyResults.toFuture(handle.result()).isSuccess()).isTrue();
    }

    @Test
    void cancellingACompletedFutureDoesNothingAndKeepsItsValue() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandConnection connection = connect(fixture, request -> request.respond(Status.OK, bytes("ok")));
        Future<RequestResult> future = NettyResults.toFuture(
                connection.request(Request.of(TYPE, bytes("x"))).result());
        AppFixture.get(AppFixture.cf(future));

        assertThat(future.cancel(false)).isFalse();
        assertThat(future.isSuccess()).isTrue();
        assertThat(future.getNow().status()).isEqualTo(Status.OK);
    }

    @Test
    void realCancellationIsDoneByTheHandleAndFuturesStillReceiveTheCancelledResult() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandConnection connection = connect(fixture, request -> {});
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("x")));
        Future<RequestResult> future = NettyResults.toFuture(handle.result());

        handle.cancel();

        assertThat(AppFixture.get(AppFixture.cf(future)).status()).isEqualTo(Status.CANCELLED);
        assertThat(future.isCancelled()).isFalse();
    }

    @Test
    void saturationFailsTheFutureKeepsTheResultAndCancellingReleasesTheReservation() throws Exception {
        AppFixture fixture = track(new AppFixture.Spec().observations(1, 64).plain());
        NettyStrandConnection connection = connect(fixture, request -> {});
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("slow")));
        Future<RequestResult> first = NettyResults.toFuture(handle.result());

        Future<RequestResult> saturated = NettyResults.toFuture(handle.result());

        assertThat(saturated.isDone()).isTrue();
        assertThat(saturated.isSuccess()).isFalse();
        assertThat(saturated.isCancelled()).isFalse();
        assertThat(saturated.cause()).isInstanceOf(ResourceExhaustedException.class);
        assertThat(handle.result().isDone()).isFalse();
        // 취소는 등록 예약을 돌려주므로 다음 변환이 다시 성공한다.
        assertThat(first.cancel(false)).isTrue();
        Future<RequestResult> again = NettyResults.toFuture(handle.result());
        assertThat(again.isDone()).isFalse();
        handle.cancel();
        assertThat(AppFixture.get(AppFixture.cf(again)).status()).isEqualTo(Status.CANCELLED);
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.CANCELLED);
    }

    @Test
    void listenerFailureIsNotRunElsewhereAndDoesNotChangeTheResultOrOtherListeners() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandConnection connection = connect(fixture, request -> request.respond(Status.OK, bytes("ok")));
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("x")));
        Future<RequestResult> future = NettyResults.toFuture(handle.result());
        List<String> threads = new CopyOnWriteArrayList<>();
        CompletableFuture<String> after = new CompletableFuture<>();
        future.addListener(done -> {
            threads.add(Thread.currentThread().getName());
            throw new IllegalStateException("listener boom");
        });
        future.addListener(done -> after.complete(Thread.currentThread().getName()));

        assertThat(after.get(15, TimeUnit.SECONDS)).startsWith("nioEventLoopGroup");
        assertThat(threads).hasSize(1).allMatch(name -> name.startsWith("nioEventLoopGroup"));
        assertThat(future.isSuccess()).isTrue();
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.OK);
    }

    @Test
    void completionAndLookupAreSeparateFromListenerExecutionWhenTheEventLoopRejects() throws Exception {
        AppFixture fixture = track(AppFixture.plain());
        NettyStrandConnection connection = connect(fixture, request -> request.respond(Status.OK, bytes("ok")));
        RequestHandle handle = connection.request(Request.of(TYPE, bytes("x")));
        awaitTrue(() -> handle.result().isDone());
        Future<RequestResult> future = NettyResults.toFuture(handle.result());
        assertThat(future.isDone()).isTrue();
        // 연결의 EventLoop가 종료된 뒤에 붙인 리스너는 거절되어 실행되지 않는다. 완료 상태와 값은 조회된다.
        fixture.group.shutdownGracefully(0, 0, TimeUnit.SECONDS).awaitUninterruptibly(15, TimeUnit.SECONDS);
        awaitTrue(() -> fixture.group.isTerminated());
        AtomicInteger ran = new AtomicInteger();
        String caller = Thread.currentThread().getName();
        future.addListener(done -> {
            ran.incrementAndGet();
            assertThat(Thread.currentThread().getName()).isNotEqualTo(caller);
        });
        Thread.sleep(200);

        assertThat(ran.get()).isZero();
        assertThat(future.isSuccess()).isTrue();
        assertThat(future.getNow().status()).isEqualTo(Status.OK);
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.OK);
    }

    @Test
    void rejectsResultsThatAreNotFromThisTransport() {
        ResultCompletion<RequestResult> foreign = new ResultCompletion<>(
                Runnable::run,
                new io.github.ppzxc.strand.api.ObservationBudget(1),
                new io.github.ppzxc.strand.api.ObservationBudget(1));
        Result<RequestResult> result = foreign;
        assertThatThrownBy(() -> NettyResults.toFuture(result)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NettyResults.toFuture(null)).isInstanceOf(NullPointerException.class);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
