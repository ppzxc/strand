package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 쓰기 대기열 상한과 읽기 멈춤. 읽지 않고 보내기만 하는 상대 앞에서 드라이버 쓰기 대기열이 상한에 묶이고(읽기를 멈추고), 시계와 명령은 계속 돌아 결국 확인
 * 대기 초과로 끊기는지, 규칙을 지키는 상대는 읽기 멈춤에 걸리지 않는지 본다. 읽지 않는 상대는 이쪽 통로의 쓰기 용량 0으로 만든다. 상대가 보내는 것은 통로를
 * 감싼 {@link Flood}가 만든다.
 */
class ConnectionDriverReadPauseTest {

    private static final int APPLICATION_TYPE = 256;
    private static final int PING_TYPE = 0x0002;
    private static final int HEADER = 20;
    private static final int MAX_FRAME_SIZE = 16_384;
    private static final long UNRESPONSIVE_ERROR_CODE = 6;
    private static final long ACK_TIMEOUT_MS = 1000;
    private static final long IDLE_INTERVAL_MS = 3000;
    private static final int PING_FRAME = HEADER + 8;
    private static final int FLOOD_SIZE = 20_000;
    private static final long PLENTY = 100_000_000;

    private static final class Side {
        final AtomicLong clock = new AtomicLong(1_000_000);
        final InMemoryPassage passage;
        final ConcurrentLinkedQueue<ConnectionClosed> closed = new ConcurrentLinkedQueue<>();
        final List<IncomingRequest> received = new CopyOnWriteArrayList<>();
        ConnectionDriver driver;

        Side(InMemoryPassage passage) {
            this.passage = passage.manualTime();
        }

        void tick(long ms) {
            clock.addAndGet(ms);
            passage.wakeup();
        }
    }

    /** 켜면 만들어 둔 프레임이 정해진 수만큼 읽혀 나오는 통로. 읽기를 기다리지 않는 대기에는 내놓지 않는다. */
    private static final class Flood implements Passage {
        private final InMemoryPassage inner;
        private final AtomicInteger remaining = new AtomicInteger();
        private final AtomicInteger delivered = new AtomicInteger();
        private volatile IntFunction<byte[]> frames = index -> new byte[0];

        Flood(InMemoryPassage inner) {
            this.inner = inner;
        }

        void flood(int count, IntFunction<byte[]> frameOf) {
            frames = frameOf;
            remaining.set(count);
            inner.wakeup();
        }

        int delivered() {
            return delivered.get();
        }

        int remaining() {
            return remaining.get();
        }

        @Override
        public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
            while (true) {
                if (waitForRead && remaining.get() > 0) {
                    remaining.decrementAndGet();
                    return Inbound.message(WebSocketMessageKind.BINARY, frames.apply(delivered.getAndIncrement()));
                }
                Inbound inbound = inner.await(timeoutMs, waitForRead, waitForWrite);
                if (inbound.kind() != Inbound.Kind.NONE || !(waitForRead && remaining.get() > 0)) {
                    return inbound;
                }
            }
        }

        @Override
        public int write(OutboundMessage message, int offset) throws IOException {
            return inner.write(message, offset);
        }

        @Override
        public void wakeup() {
            inner.wakeup();
        }

        @Override
        public void shutdownOutput() {
            inner.shutdownOutput();
        }

        @Override
        public void close() {
            inner.close();
        }
    }

    private final List<ConnectionDriver> started = new ArrayList<>();
    private final ExecutorService handlers = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "test-handler");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void stopAll() {
        started.forEach(ConnectionDriver::close);
        handlers.shutdownNow();
    }

    private static EndpointConfig config(long maxConcurrentStreams) {
        return EndpointConfig.defaults()
                .webSocket()
                .maxFrameSize(MAX_FRAME_SIZE)
                .maxConcurrentStreams(maxConcurrentStreams)
                .ackTimeoutMs(ACK_TIMEOUT_MS)
                .idleIntervalMs(IDLE_INTERVAL_MS);
    }

    private void start(Side side, Passage passage, EndpointConfig config, boolean client, RequestHandler handler) {
        ConnectionListener listener = new ConnectionListener() {
            @Override
            public void onClosed(ConnectionClosed event) {
                side.closed.add(event);
            }
        };
        ConnectionDriver.Builder builder =
                client ? ConnectionDriver.client(config, passage) : ConnectionDriver.server(config, passage);
        side.driver = builder.handler(handlers, handler)
                .listener(listener)
                .clock(side.clock::get)
                .start();
        started.add(side.driver);
    }

    private void settle(Side... sides) {
        InMemoryPassage[] passages = new InMemoryPassage[sides.length];
        for (int i = 0; i < sides.length; i++) {
            passages[i] = sides[i].passage;
        }
        InMemoryPassage.awaitQuiescence(passages);
        try {
            handlers.submit(() -> {}).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IllegalStateException("핸들러 실행기가 비지 않았다", e);
        }
        InMemoryPassage.awaitQuiescence(passages);
    }

    /** 핸드셰이크를 마친 한 쌍. 클라이언트 통로를 {@link Flood}로 감싸 돌려준다. */
    private Side[] connected(EndpointConfig clientConfig, EndpointConfig serverConfig, RequestHandler clientHandler) {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        start(server, server.passage, serverConfig, false, server.received::add);
        start(client, client.passage, clientConfig, true, clientHandler);
        settle(client, server);
        return new Side[] {client, server};
    }

    private Flood floodingClient(
            EndpointConfig clientConfig, EndpointConfig serverConfig, RequestHandler handler, Side[] out) {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        Flood flood = new Flood(client.passage);
        start(server, server.passage, serverConfig, false, server.received::add);
        start(client, flood, clientConfig, true, handler);
        settle(client, server);
        out[0] = client;
        out[1] = server;
        return flood;
    }

    private static byte[] frame(int type, int flags, int status, long streamId, byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int length = HEADER - 4 + payload.length;
        out.write(length >>> 24);
        out.write(length >>> 16);
        out.write(length >>> 8);
        out.write(length);
        out.write(type >>> 8);
        out.write(type);
        out.write(flags);
        out.write(status);
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.write((int) (streamId >>> shift));
        }
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(payload, 0, payload.length);
        return out.toByteArray();
    }

    private static byte[] ping(int index) {
        return frame(PING_TYPE, 0, 0, 0, new byte[8]);
    }

    /** 상대(서버) 쪽 번호 공간의 요청. 번호는 2부터 2씩 는다. */
    private static byte[] request(int index) {
        return frame(APPLICATION_TYPE, 0, 0, 2L * (index + 1), new byte[100]);
    }

    private static void assertUnresponsive(Side side) {
        assertThat(side.closed).hasSize(1);
        CloseReason reason = side.closed.peek().reason();
        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
        assertThat(reason.errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
    }

    /**
     * 읽기를 멈추는 한계의 바이트 수. (상대가 알린 동시 스트림 + 내가 알린 동시 스트림) × 상대가 알린 최대 프레임 + 프레임 하나. 두 끝의 설정이 같다.
     */
    private static long limit(long maxConcurrentStreams) {
        return 2 * maxConcurrentStreams * MAX_FRAME_SIZE + MAX_FRAME_SIZE;
    }

    @Test
    void aPeerThatSendsOnlyPingsWithoutReadingIsReadOnlyUpToTheQueueLimitAndThenTheConnectionEnds() {
        Side[] sides = new Side[2];
        Flood flood = floodingClient(config(2), config(2), request -> {}, sides);
        Side client = sides[0];
        Side server = sides[1];
        client.passage.writeCapacity(0);

        flood.flood(FLOOD_SIZE, ConnectionDriverReadPauseTest::ping);
        settle(client, server);

        // ACK PING이 28바이트씩 쓰기 대기열에 쌓인다. 상한에서 읽기를 멈추므로 끝까지 읽지 않는다.
        long limitFrames = limit(2) / PING_FRAME;
        assertThat(flood.delivered()).isBetween((int) limitFrames - 10, (int) limitFrames + 64 + 10);
        assertThat(flood.remaining()).isPositive();

        // 읽기를 멈춰도 시계는 간다. 받는 것이 없으니 유휴 PING이 나가고 확인 대기가 초과된다.
        client.tick(IDLE_INTERVAL_MS);
        settle(client, server);
        client.tick(ACK_TIMEOUT_MS);
        settle(client, server);
        assertThat(client.closed).isEmpty();
        client.tick(1);
        settle(client, server);
        assertUnresponsive(client);
    }

    @Test
    void aPeerThatSendsOnlyRequestsWithoutReadingEndsTheConnectionOnTheRefusalPath() {
        Side[] sides = new Side[2];
        // 동시 스트림 0이라 상대 요청은 모두 RESET(REFUSED)이 쓰기 대기열에 쌓인다.
        Flood flood = floodingClient(config(0), config(2), request -> {}, sides);
        Side client = sides[0];
        Side server = sides[1];
        client.passage.writeCapacity(0);

        flood.flood(FLOOD_SIZE, ConnectionDriverReadPauseTest::request);
        settle(client, server);

        // RESET(REFUSED) 한 프레임이 대기열에 쌓이는 만큼 읽고 멈춘다. 상한 49152바이트에 못 미치게 읽거나 크게 넘지 않는다.
        assertThat(flood.delivered()).isBetween(1000, 2300);
        assertThat(flood.remaining()).isPositive();

        client.tick(IDLE_INTERVAL_MS);
        settle(client, server);
        client.tick(ACK_TIMEOUT_MS + 1);
        settle(client, server);
        assertUnresponsive(client);
    }

    @Test
    void aPeerThatSendsOnlyRequestsWithoutReadingEndsTheConnectionOnTheResponsePath() {
        Side[] sides = new Side[2];
        Flood flood = floodingClient(
                config(2),
                config(2),
                request -> {
                    // 핸들러가 바로 응답하면 슬롯이 비어 상대 요청이 다시 받아들여지고, 응답이 쌓인다.
                    CompletableFuture<Void> unused = request.respond(0, new byte[100]);
                },
                sides);
        Side client = sides[0];
        Side server = sides[1];
        client.passage.writeCapacity(0);

        flood.flood(FLOOD_SIZE, ConnectionDriverReadPauseTest::request);
        settle(client, server);

        // 응답(120바이트)과 한도를 넘은 요청의 REFUSED(24바이트)가 섞여 쌓이므로 정확한 수는 핸들러 스레드 속도에 달렸다. 상한 81920바이트 안팎에서 멈춘다.
        assertThat(flood.delivered()).isBetween(300, 5000);
        assertThat(flood.remaining()).isPositive();

        client.tick(IDLE_INTERVAL_MS);
        settle(client, server);
        client.tick(ACK_TIMEOUT_MS + 1);
        settle(client, server);
        assertUnresponsive(client);
    }

    @Test
    void readingStartsAgainWhenTheQueueDropsBelowTheLimit() {
        Side[] sides = new Side[2];
        Flood flood = floodingClient(config(2), config(2), request -> {}, sides);
        Side client = sides[0];
        Side server = sides[1];
        client.passage.writeCapacity(0);
        flood.flood(FLOOD_SIZE, ConnectionDriverReadPauseTest::ping);
        settle(client, server);
        assertThat(flood.remaining()).isPositive();

        client.passage.addWriteCapacity(PLENTY);
        settle(client, server);

        assertThat(flood.remaining()).isZero();
        assertThat(flood.delivered()).isEqualTo(FLOOD_SIZE);
        assertThat(client.closed).isEmpty();
    }

    @Test
    void twoCompliantPeersThatFillTheFlowControlLimitDoNotTripTheReadPauseOrDeadlock() throws Exception {
        long concurrent = 2;
        List<IncomingRequest> clientReceived = new CopyOnWriteArrayList<>();
        Side[] sides = connected(config(concurrent), config(concurrent), clientReceived::add);
        Side client = sides[0];
        Side server = sides[1];
        byte[] body = new byte[MAX_FRAME_SIZE - HEADER];
        // 상대가 읽지 않는 것처럼 클라이언트 쓰기를 막은 채, 내 요청 둘과 상대 요청 둘에 대한 응답 둘(프레임 넷, 상한은 다섯)을 대기열에 채운다.
        client.passage.writeCapacity(0);
        for (int i = 0; i < concurrent; i++) {
            client.driver.request(APPLICATION_TYPE, body);
        }
        List<RequestHandle> fromServer = new ArrayList<>();
        for (int i = 0; i < concurrent; i++) {
            fromServer.add(server.driver.request(APPLICATION_TYPE, body));
        }
        settle(client, server);
        assertThat(clientReceived).hasSize((int) concurrent);
        for (IncomingRequest request : clientReceived) {
            request.respond(0, body).get(5, TimeUnit.SECONDS);
        }
        settle(client, server);

        // 대기열이 정상 범위를 꽉 채운 채여도 읽기는 계속된다.
        long before = client.driver.receivedBytes();
        server.driver.ping(new byte[8]).get(5, TimeUnit.SECONDS);
        settle(client, server);
        assertThat(client.driver.receivedBytes()).isEqualTo(before + PING_FRAME);

        client.passage.addWriteCapacity(PLENTY);
        settle(client, server);
        assertThat(client.closed).isEmpty();
        assertThat(server.closed).isEmpty();
        assertThat(fromServer).allSatisfy(handle -> assertThat(handle.result()).isDone());
        assertThat(server.received).hasSize((int) concurrent);
    }

    @Test
    void theLimitIsTheFlowControlRangePlusOneFrame() {
        // (상대가 알린 동시 스트림 + 내가 알린 동시 스트림) × 상대가 알린 최대 프레임 + 프레임 하나.
        assertThat(ConnectionDriver.writeQueueLimit(65_536, 100, 100, PLENTY)).isEqualTo(200L * 65_536 + 65_536);
        assertThat(ConnectionDriver.writeQueueLimit(16_384, 2, 2, PLENTY)).isEqualTo(5L * 16_384);
        // 동시 스트림이 0이어도 제어 프레임을 위한 프레임 하나는 남는다.
        assertThat(ConnectionDriver.writeQueueLimit(16_384, 0, 0, PLENTY)).isEqualTo(16_384);
    }

    @Test
    void aPeerCannotRaiseTheLimitWithoutBoundByAdvertisingHugeValues() {
        long hugeStreams = 4_294_967_295L;
        long hugeFrame = 1_048_576L;
        assertThat(ConnectionDriver.writeQueueLimit(
                        hugeFrame, hugeStreams, hugeStreams, ConnectionDriver.DEFAULT_MAX_WRITE_QUEUE_BYTES))
                .isEqualTo(ConnectionDriver.DEFAULT_MAX_WRITE_QUEUE_BYTES);
        // 연결 설정으로 절대 한도를 올리면 정상 범위가 그만큼 열린다.
        assertThat(ConnectionDriver.writeQueueLimit(hugeFrame, 100, 100, 1L << 40))
                .isEqualTo(201L * hugeFrame);
    }

    @Test
    void theLimitIsNeverBelowOneFrameEvenWithASmallAbsoluteLimit() {
        assertThat(ConnectionDriver.writeQueueLimit(1_048_576, 100, 100, 1)).isEqualTo(1_048_576);
    }

    @Test
    void theAbsoluteLimitCannotBeBelowTheSmallestFrameSize() {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        assertThatThrownBy(
                        () -> ConnectionDriver.client(config(2), link.first()).maxWriteQueueBytes(MAX_FRAME_SIZE - 1))
                .isInstanceOf(IllegalArgumentException.class);
        // 하한은 그 값 자체를 받는다.
        ConnectionDriver.client(config(2), link.first()).maxWriteQueueBytes(MAX_FRAME_SIZE);
    }

    @Test
    void theAbsoluteLimitFromTheBuilderReachesTheDriver() {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        Flood flood = new Flood(client.passage);
        start(server, server.passage, config(2), false, server.received::add);
        client.driver = ConnectionDriver.client(config(2), flood)
                .handler(handlers, request -> {})
                .maxWriteQueueBytes(MAX_FRAME_SIZE)
                .clock(client.clock::get)
                .start();
        started.add(client.driver);
        settle(client, server);
        client.passage.writeCapacity(0);

        flood.flood(FLOOD_SIZE, ConnectionDriverReadPauseTest::ping);
        settle(client, server);

        // 협상 값에서 셈한 상한은 81920이지만 연결 설정의 절대 한도 16384가 먼저 걸린다.
        int capFrames = MAX_FRAME_SIZE / PING_FRAME;
        assertThat(flood.delivered()).isBetween(capFrames - 10, capFrames + 64 + 10);
    }

    @Test
    void theTransportThreadDoesNotWakeUpEmptyWhileReadingIsPaused() {
        Side[] sides = connected(config(2), config(2), request -> {});
        Side client = sides[0];
        Side server = sides[1];
        client.passage.writeCapacity(0);
        int pings = (int) (limit(2) / PING_FRAME) * 2;
        List<CompletableFuture<Void>> sent = new ArrayList<>();
        for (int i = 0; i < pings; i++) {
            sent.add(server.driver.ping(new byte[8]));
        }
        try {
            CompletableFuture.allOf(sent.toArray(new CompletableFuture<?>[0])).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IllegalStateException("PING 명령이 처리되지 않았다", e);
        }
        settle(client, server);

        // 읽기가 실제로 멈췄다: 서버가 보낸 PING을 다 받지 않았다.
        assertThat(client.driver.receivedBytes()).isLessThan((long) pings * PING_FRAME);
        // 읽을 것이 남아 있어도 읽기를 멈춘 동안은 쓰기 가능, 깨움, 기한으로만 깨어난다.
        long calls = client.passage.awaitCalls();
        settle(client, server);
        // 되풀이해 깨어난다면 실제 시간이 흐르는 동안 호출 수가 늘어난다.
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(client.passage.awaitCalls()).isEqualTo(calls);
    }
}
