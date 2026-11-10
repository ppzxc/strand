package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * 전송 모듈의 interface(요청 → Future, 핸들러)로만 관찰한다. 두 연결은 in-memory 통로로 이어 붙인다. 같은 시나리오를 바이트 스트림 통로와 메시지
 * 통로(WebSocket 설정)로 모두 돈다.
 */
class ConnectionDriverTest {

    private static final int APPLICATION_TYPE = 256;
    private static final int STATUS_OK = 0;
    private static final int STATUS_UNAVAILABLE = 14;

    /** 통로 종류와 거기에 맞는 엔드포인트 설정. */
    enum Wire {
        STREAM {
            @Override
            InMemoryPassage.Pair connect() {
                return InMemoryPassage.streams();
            }

            @Override
            EndpointConfig config() {
                return EndpointConfig.defaults();
            }
        },
        MESSAGE {
            @Override
            InMemoryPassage.Pair connect() {
                return InMemoryPassage.messages();
            }

            @Override
            EndpointConfig config() {
                return EndpointConfig.defaults().webSocket();
            }
        };

        abstract InMemoryPassage.Pair connect();

        abstract EndpointConfig config();
    }

    private final List<ConnectionDriver> started = new ArrayList<>();
    private final ExecutorService handlers = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "test-handler");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void stopAll() {
        started.forEach(ConnectionDriver::close);
        handlers.shutdownNow();
    }

    private ConnectionDriver server(Wire wire, Passage passage, RequestHandler handler) {
        return start(ConnectionDriver.server(wire.config(), passage).handler(handlers, handler));
    }

    private ConnectionDriver client(Wire wire, Passage passage, RequestHandler handler) {
        return start(ConnectionDriver.client(wire.config(), passage).handler(handlers, handler));
    }

    private ConnectionDriver start(ConnectionDriver.Builder builder) {
        // 시계는 주입한다. 이 티켓에서는 시계 진행이 없으므로 값은 쓰이지 않는다.
        ConnectionDriver driver = builder.clock(() -> 0L).start();
        started.add(driver);
        return driver;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
    }

    /** 받은 요청 본문 앞에 "re:"를 붙여 응답하는 핸들러. */
    private static void echo(IncomingRequest request) {
        request.respond(STATUS_OK, bytes("re:" + text(request.body()))).join();
    }

    private static void ignore(IncomingRequest request) {}

    @ParameterizedTest
    @EnumSource(Wire.class)
    void clientRequestReceivesTheServerHandlersResponse(Wire wire) throws Exception {
        InMemoryPassage.Pair link = wire.connect();
        server(wire, link.second(), ConnectionDriverTest::echo);
        ConnectionDriver client = client(wire, link.first(), ConnectionDriverTest::ignore);

        RequestResult result = await(client.request(APPLICATION_TYPE, bytes("hello")));

        assertThat(result.status()).isEqualTo(STATUS_OK);
        assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(text(result.body())).isEqualTo("re:hello");
    }

    @ParameterizedTest
    @EnumSource(Wire.class)
    void serverRequestReceivesTheClientHandlersResponse(Wire wire) throws Exception {
        InMemoryPassage.Pair link = wire.connect();
        ConnectionDriver server = server(wire, link.second(), ConnectionDriverTest::ignore);
        client(wire, link.first(), ConnectionDriverTest::echo);

        RequestResult result = await(server.request(APPLICATION_TYPE + 1, bytes("from-server")));

        assertThat(result.status()).isEqualTo(STATUS_OK);
        assertThat(text(result.body())).isEqualTo("re:from-server");
    }

    @ParameterizedTest
    @EnumSource(Wire.class)
    void requestIssuedBeforeThePeerStartsReceivesItsResultAfterTheAuthGate(Wire wire) throws Exception {
        InMemoryPassage.Pair link = wire.connect();
        ConnectionDriver client = client(wire, link.first(), ConnectionDriverTest::ignore);
        CompletableFuture<RequestResult> early = client.request(APPLICATION_TYPE, bytes("early"));
        assertThat(early).isNotDone();

        server(wire, link.second(), ConnectionDriverTest::echo);

        assertThat(text(await(early).body())).isEqualTo("re:early");
    }

    @ParameterizedTest
    @EnumSource(Wire.class)
    void everyResultReachesItsOwnRequestWhenManyAreInFlight(Wire wire) throws Exception {
        InMemoryPassage.Pair link = wire.connect();
        server(wire, link.second(), ConnectionDriverTest::echo);
        ConnectionDriver client = client(wire, link.first(), ConnectionDriverTest::ignore);

        List<CompletableFuture<RequestResult>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            futures.add(client.request(APPLICATION_TYPE, bytes("n" + i)));
        }

        for (int i = 0; i < 50; i++) {
            assertThat(text(await(futures.get(i)).body())).isEqualTo("re:n" + i);
        }
    }

    @ParameterizedTest
    @EnumSource(Wire.class)
    void handlerRunsOnTheGivenExecutorWithTypeAndBodyAndMayRespondFromAnotherThread(Wire wire) throws Exception {
        InMemoryPassage.Pair link = wire.connect();
        AtomicReference<String> handlerThread = new AtomicReference<>();
        AtomicReference<Integer> seenType = new AtomicReference<>();
        AtomicReference<String> seenBody = new AtomicReference<>();
        AtomicReference<String> responderThread = new AtomicReference<>();
        ExecutorService responders = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "test-responder");
            thread.setDaemon(true);
            return thread;
        });
        try {
            server(wire, link.second(), request -> {
                handlerThread.set(Thread.currentThread().getName());
                seenType.set(request.type());
                seenBody.set(text(request.body()));
                responders.execute(() -> {
                    responderThread.set(Thread.currentThread().getName());
                    request.respond(STATUS_OK, bytes("late")).join();
                });
            });
            ConnectionDriver client = client(wire, link.first(), ConnectionDriverTest::ignore);

            RequestResult result = await(client.request(APPLICATION_TYPE + 7, bytes("payload")));

            assertThat(text(result.body())).isEqualTo("late");
            assertThat(handlerThread.get()).isEqualTo("test-handler");
            assertThat(responderThread.get()).isEqualTo("test-responder");
            assertThat(seenType.get()).isEqualTo(APPLICATION_TYPE + 7);
            assertThat(seenBody.get()).isEqualTo("payload");
        } finally {
            responders.shutdownNow();
        }
    }

    @ParameterizedTest
    @EnumSource(Wire.class)
    void requestsAndResponsesFromManyThreadsAllArrive(Wire wire) throws Exception {
        InMemoryPassage.Pair link = wire.connect();
        ConnectionDriver server = server(wire, link.second(), ConnectionDriverTest::echo);
        ConnectionDriver client = client(wire, link.first(), ConnectionDriverTest::echo);
        int threads = 8;
        int perThread = 25;
        CountDownLatch go = new CountDownLatch(1);
        List<CompletableFuture<Void>> checks = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int id = t;
            ConnectionDriver sender = t % 2 == 0 ? client : server;
            checks.add(CompletableFuture.runAsync(
                    () -> {
                        try {
                            go.await();
                            for (int i = 0; i < perThread; i++) {
                                String body = id + "-" + i;
                                RequestResult result = await(sender.request(APPLICATION_TYPE, bytes(body)));
                                assertThat(text(result.body())).isEqualTo("re:" + body);
                            }
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    },
                    handlers));
        }
        go.countDown();

        for (CompletableFuture<Void> check : checks) {
            await(check);
        }
    }

    @Test
    void whenThePeerClosesThePassagePendingRequestsEndWithAnUnavailableResult() throws Exception {
        Wire wire = Wire.STREAM;
        InMemoryPassage.Pair link = wire.connect();
        CountDownLatch received = new CountDownLatch(1);
        server(wire, link.second(), request -> received.countDown());
        ConnectionDriver client = client(wire, link.first(), ConnectionDriverTest::ignore);
        CompletableFuture<RequestResult> pending = client.request(APPLICATION_TYPE, bytes("unanswered"));
        assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();

        link.second().close();

        RequestResult result = await(pending);
        assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
        assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    @Test
    void requestsAfterTheConnectionEndedFailTheirFuture() {
        Wire wire = Wire.STREAM;
        InMemoryPassage.Pair link = wire.connect();
        ConnectionDriver client = client(wire, link.first(), ConnectionDriverTest::ignore);
        client.close();

        CompletableFuture<RequestResult> late = client.request(APPLICATION_TYPE, bytes("late"));

        assertThat(late).isCompletedExceptionally();
    }

    @Test
    void closingTheOwnPassageEndsPendingRequests() throws Exception {
        Wire wire = Wire.STREAM;
        InMemoryPassage.Pair link = wire.connect();
        CountDownLatch received = new CountDownLatch(1);
        server(wire, link.second(), request -> received.countDown());
        ConnectionDriver client = client(wire, link.first(), ConnectionDriverTest::ignore);
        CompletableFuture<RequestResult> pending = client.request(APPLICATION_TYPE, bytes("unanswered"));
        assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();

        link.first().close();

        assertThat(await(pending).status()).isEqualTo(STATUS_UNAVAILABLE);
    }

    /** {@code failWrites}가 켜지면 쓰기가 실패하는 통로. */
    private static final class UnwritablePassage implements Passage {
        private final Passage delegate;
        final AtomicBoolean failWrites = new AtomicBoolean();

        UnwritablePassage(Passage delegate) {
            this.delegate = delegate;
        }

        @Override
        public Inbound await(long timeoutMs) throws IOException {
            return delegate.await(timeoutMs);
        }

        @Override
        public void write(OutboundMessage message) throws IOException {
            if (failWrites.get()) {
                throw new IOException("쓸 수 없음");
            }
            delegate.write(message);
        }

        @Override
        public void wakeup() {
            delegate.wakeup();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    @Test
    void aWriteFailureEndsTheConnectionAndTheRequestsWaitingOnIt() throws Exception {
        Wire wire = Wire.STREAM;
        InMemoryPassage.Pair link = wire.connect();
        UnwritablePassage unwritable = new UnwritablePassage(link.first());
        server(wire, link.second(), ConnectionDriverTest::echo);
        ConnectionDriver client = client(wire, unwritable, ConnectionDriverTest::ignore);
        await(client.request(APPLICATION_TYPE, bytes("warm-up")));
        unwritable.failWrites.set(true);

        RequestResult result = await(client.request(APPLICATION_TYPE, bytes("unsendable")));

        assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
    }

    @Test
    void aServerWithoutAHandlerLeavesRequestsUnansweredUntilTheConnectionEnds() throws Exception {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        ConnectionDriver server = start(ConnectionDriver.server(EndpointConfig.defaults(), link.second()));
        ConnectionDriver client = client(Wire.STREAM, link.first(), ConnectionDriverTest::echo);
        CompletableFuture<RequestResult> pending = client.request(APPLICATION_TYPE, bytes("nobody-home"));
        // 서버 쪽 요청이 결과를 받으면 연결은 수립된 것이다.
        assertThat(await(server.request(APPLICATION_TYPE, bytes("ping"))).status())
                .isEqualTo(STATUS_OK);

        server.close();

        assertThat(await(pending).status()).isEqualTo(STATUS_UNAVAILABLE);
    }

    @Test
    void aHandlerMayCloseTheConnectionFromTheTransportThread() throws Exception {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        AtomicReference<ConnectionDriver> server = new AtomicReference<>();
        server.set(start(ConnectionDriver.server(EndpointConfig.defaults(), link.second())
                .handler(Runnable::run, request -> {
                    // 같은 스레드에서 닫아도 자기 자신을 기다리다 멈추지 않는다.
                    server.get().close();
                })));
        ConnectionDriver client = client(Wire.STREAM, link.first(), ConnectionDriverTest::ignore);

        RequestResult result = await(client.request(APPLICATION_TYPE, bytes("close-me")));

        assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
    }

    @Test
    void closeKeepsTheCallersInterruptFlag() {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        ConnectionDriver client = client(Wire.STREAM, link.first(), ConnectionDriverTest::ignore);

        Thread.currentThread().interrupt();
        client.close();

        assertThat(Thread.interrupted()).isTrue();
        client.close();
    }

    @Test
    void respondingAfterTheConnectionEndedFailsItsFuture() {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        ConnectionDriver client = client(Wire.STREAM, link.first(), ConnectionDriverTest::ignore);
        client.close();

        assertThat(client.respond(2, STATUS_OK, bytes("late"))).isCompletedExceptionally();
    }

    @Test
    void aRequestLargerThanThePeersMaxFrameSizeFailsItsFutureAndTheConnectionStaysUsable() throws Exception {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        start(ConnectionDriver.server(EndpointConfig.defaults().maxFrameSize(16384), link.second())
                .handler(handlers, ConnectionDriverTest::echo));
        ConnectionDriver client = client(Wire.STREAM, link.first(), ConnectionDriverTest::ignore);
        await(client.request(APPLICATION_TYPE, bytes("warm-up")));

        CompletableFuture<RequestResult> tooBig = client.request(APPLICATION_TYPE, new byte[20000]);

        assertThat(tooBig)
                .failsWithin(5, TimeUnit.SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(IllegalArgumentException.class);
        assertThat(text(await(client.request(APPLICATION_TYPE, bytes("after")).thenApply(RequestResult::body))))
                .isEqualTo("re:after");
    }

    @Test
    void aResponseLargerThanThePeersMaxFrameSizeFailsItsFutureSoTheHandlerCanRespondAgainSmaller() throws Exception {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        start(ConnectionDriver.server(EndpointConfig.defaults(), link.second()).handler(handlers, request -> {
            CompletableFuture<Void> big = request.respond(STATUS_OK, new byte[20000]);
            try {
                big.get(5, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                request.respond(STATUS_OK, bytes("small")).join();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }));
        start(ConnectionDriver.client(EndpointConfig.defaults().maxFrameSize(16384), link.first()));
        ConnectionDriver client = started.get(1);

        RequestResult result = await(client.request(APPLICATION_TYPE, bytes("big-please")));

        assertThat(text(result.body())).isEqualTo("small");
    }

    @Test
    void aRejectingHandlerExecutorEndsTheDriverAndFailsItsOwnPendingRequests() throws Exception {
        InMemoryPassage.Pair link = InMemoryPassage.streams();
        ConnectionDriver server = start(ConnectionDriver.server(EndpointConfig.defaults(), link.second())
                .handler(
                        runnable -> {
                            throw new RejectedExecutionException("가득 참");
                        },
                        ConnectionDriverTest::ignore));
        ConnectionDriver client = client(Wire.STREAM, link.first(), ConnectionDriverTest::ignore);
        CompletableFuture<RequestResult> serverOwn = server.request(APPLICATION_TYPE, bytes("server-own"));
        CompletableFuture<RequestResult> clientOwn = client.request(APPLICATION_TYPE, bytes("trigger"));

        assertThat(serverOwn).failsWithin(5, TimeUnit.SECONDS);
        assertThat(await(clientOwn).status()).isEqualTo(STATUS_UNAVAILABLE);
    }
}
