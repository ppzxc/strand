package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * 시계와 기한. 연결 드라이버에는 단조 시계를 주입하고, 통로는 시간 제한이 실제 시간으로 흐르지 않는 {@link InMemoryPassage#manualTime}이다. 테스트가
 * 시계를 올리고 통로를 깨운 뒤 양쪽이 가라앉기를 기다리므로 실제 시간을 기다리지 않는다. 기한·확인 대기·AUTH 대기는 바이트 스트림 통로와 메시지 통로 모두로 돈다. PING을 세는 시험(유휴 PING, 앱 PING)은 프레임 하나가 쓰기 하나인
 * 메시지 통로에서만 한다. 바이트 스트림 통로는 서문과 프레임이 어떻게 묶여 쓰이는지에 기대지 않으려는 것이다.
 */
class ConnectionDriverClockTest {

    private static final int APPLICATION_TYPE = 256;
    private static final int STATUS_DEADLINE_EXCEEDED = 4;
    private static final int STATUS_UNAVAILABLE = 14;
    private static final long FLOOD_SIZE = 20_000;
    private static final int PING_FRAME_TYPE = 0x0002;
    private static final int ACK_FLAG = 0x01;
    private static final int FRAME_HEADER_SIZE = 20;

    /** 연결 드라이버와 그 시계·통로. */
    private static final class Side {
        final AtomicLong clock = new AtomicLong(1_000_000);
        final InMemoryPassage passage;
        ConnectionDriver driver;

        Side(InMemoryPassage passage) {
            this.passage = passage.manualTime();
        }

        /** 시계를 올리고 전송 스레드를 깨운다. */
        void tick(long ms) {
            clock.addAndGet(ms);
            passage.wakeup();
        }
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

    private static EndpointConfig config() {
        return EndpointConfig.defaults().webSocket();
    }

    private Side startClient(Side side, EndpointConfig config) {
        return startClient(side, config, request -> {});
    }

    private Side startClient(Side side, EndpointConfig config, RequestHandler handler) {
        side.driver = ConnectionDriver.client(config, side.passage)
                .handler(handlers, handler)
                .clock(side.clock::get)
                .start();
        started.add(side.driver);
        return side;
    }

    private Side startServer(Side side, EndpointConfig config) {
        return startServer(side, config, request -> {});
    }

    private Side startServer(Side side, EndpointConfig config, RequestHandler handler) {
        side.driver = ConnectionDriver.server(config, side.passage)
                .handler(handlers, handler)
                .clock(side.clock::get)
                .start();
        started.add(side.driver);
        return side;
    }

    private static void settle(Side... sides) {
        InMemoryPassage[] passages = new InMemoryPassage[sides.length];
        for (int i = 0; i < sides.length; i++) {
            passages[i] = sides[i].passage;
        }
        InMemoryPassage.awaitQuiescence(passages);
    }

    /** 통로로 쓴 프레임 가운데 PING(ACK 아님)의 payload들. */
    private static List<byte[]> pingsWritten(InMemoryPassage passage) {
        List<byte[]> pings = new ArrayList<>();
        for (byte[] frame : passage.written()) {
            int type = ((frame[4] & 0xFF) << 8) | (frame[5] & 0xFF);
            boolean ack = (frame[6] & ACK_FLAG) != 0;
            if (type == PING_FRAME_TYPE && !ack) {
                pings.add(Arrays.copyOfRange(frame, FRAME_HEADER_SIZE, frame.length));
            }
        }
        return pings;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @ParameterizedTest
    @EnumSource(Wire.class)
    void aRequestPastItsDeadlineEndsWithDeadlineExceededAndAnUnknownResultOnceWritten(Wire wire) {
        InMemoryPassage.Pair link = wire.connect();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        startServer(server, wire.config());
        startClient(client, wire.config());
        RequestHandle handle = client.driver.request(APPLICATION_TYPE, bytes("slow"), 100);
        settle(client, server);

        client.tick(100);
        settle(client, server);
        assertThat(handle.result()).isNotDone();

        client.tick(1);
        settle(client, server);
        assertThat(handle.result()).isDone();
        RequestResult result = handle.result().join();
        assertThat(result.status()).isEqualTo(STATUS_DEADLINE_EXCEEDED);
        assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    @ParameterizedTest
    @EnumSource(Wire.class)
    void aRequestPastItsDeadlineBeforeAnythingWasWrittenEndsAsNotProcessed(Wire wire) {
        InMemoryPassage.Pair link = wire.connect();
        Side client = new Side(link.first());
        startClient(client, wire.config());
        // 서버가 없어 AUTH 관문을 지나지 못하므로 요청은 쓰이지 않은 채 미뤄져 있다.
        RequestHandle handle = client.driver.request(APPLICATION_TYPE, bytes("never-sent"), 100);
        settle(client);

        client.tick(101);
        settle(client);

        assertThat(handle.result()).isDone();
        RequestResult result = handle.result().join();
        assertThat(result.status()).isEqualTo(STATUS_DEADLINE_EXCEEDED);
        assertThat(result.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
    }

    @ParameterizedTest
    @EnumSource(Wire.class)
    void whenThePeerNeverAcknowledgesTheConnectionEndsAndEveryPendingFutureEnds(Wire wire) {
        InMemoryPassage.Pair link = wire.connect();
        Side client = new Side(link.first());
        startClient(client, wire.config().ackTimeoutMs(1000));
        RequestHandle first = client.driver.request(APPLICATION_TYPE, bytes("a"));
        RequestHandle second = client.driver.request(APPLICATION_TYPE, bytes("b"));
        settle(client);

        client.tick(1000);
        settle(client);
        assertThat(first.result()).isNotDone();

        client.tick(1);
        settle(client);
        assertThat(first.result()).isDone();
        assertThat(second.result()).isDone();
        assertThat(first.result().join().status()).isEqualTo(STATUS_UNAVAILABLE);
        assertThat(second.result().join().status()).isEqualTo(STATUS_UNAVAILABLE);
    }

    @ParameterizedTest
    @EnumSource(Wire.class)
    void anAcknowledgementReadInTheSameStepAsTheElapsedTimeIsNotMistakenForATimeout(Wire wire) throws Exception {
        InMemoryPassage.Pair link = wire.connect();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        startClient(client, wire.config().ackTimeoutMs(1000));
        RequestHandle handle = client.driver.request(APPLICATION_TYPE, bytes("hello"));
        settle(client);
        // 클라이언트가 자는 동안 시계만 확인 대기 기한을 훌쩍 넘기고, 그 사이 서버가 ACK를 보낸다.
        client.clock.addAndGet(5000);

        startServer(
                server,
                wire.config(),
                request -> request.respond(0, bytes("re:hello")).join());
        settle(client, server);

        // 핸들러는 다른 스레드에서 돌아 응답이 가라앉은 뒤에 올 수 있으므로 결과를 기다린다(시계와 무관하다).
        assertThat(handle.result().get(5, TimeUnit.SECONDS).certainty()).isEqualTo(Certainty.RESPONSE);
    }

    @ParameterizedTest
    @EnumSource(Wire.class)
    void aServerThatNeverGetsAuthEndsTheConnectionWhenTheAuthWaitRunsOut(Wire wire) {
        InMemoryPassage.Pair link = wire.connect();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        startClient(client, wire.config());
        settle(client);
        // 클라이언트는 HELLO까지만 나가고 그 뒤 말이 없다. AUTH가 오지 않는다.
        client.passage.mute();
        startServer(server, wire.config().ackTimeoutMs(1_000_000).authTimeoutMs(700));
        RequestHandle handle = server.driver.request(APPLICATION_TYPE, bytes("early"));
        settle(client, server);

        server.tick(700);
        settle(client, server);
        assertThat(handle.result()).isNotDone();

        server.tick(1);
        settle(client, server);
        assertThat(handle.result()).isDone();
        assertThat(handle.result().join().status()).isEqualTo(STATUS_UNAVAILABLE);
    }

    @Test
    void anIdleConnectionSendsAnIdlePing() {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        startServer(server, config());
        startClient(client, config().idleIntervalMs(3000));
        settle(client, server);
        assertThat(pingsWritten(client.passage)).isEmpty();

        client.tick(2999);
        settle(client, server);
        assertThat(pingsWritten(client.passage)).isEmpty();

        client.tick(1);
        settle(client, server);
        assertThat(pingsWritten(client.passage)).hasSize(1);
    }

    @Test
    void anApplicationPingIsDeferredUntilTheAuthGateOpensAndThenGoesOut() {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        startClient(client, config());
        client.driver.ping(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}).join();
        settle(client);
        assertThat(pingsWritten(client.passage)).isEmpty();

        startServer(server, config());
        settle(client, server);

        assertThat(pingsWritten(client.passage)).hasSize(1);
        assertThat(pingsWritten(client.passage).get(0)).containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
    }

    @Test
    void theTransportThreadWaitsWithoutALimitWhenNothingHasADeadlineAndOnlyUntilTheNextDeadlineOtherwise() {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        startServer(server, config());
        startClient(client, config());
        settle(client, server);
        assertThat(client.passage.lastAwaitTimeoutMs()).isNegative();
        assertThat(server.passage.lastAwaitTimeoutMs()).isNegative();
        // 시계가 가지 않는 동안 await를 되풀이해 부르지 않는다(0 시간 제한의 바쁜 대기가 없다).
        long clientAwaits = client.passage.awaitCalls();
        long serverAwaits = server.passage.awaitCalls();
        settle(client, server);
        assertThat(client.passage.awaitCalls()).isEqualTo(clientAwaits);
        assertThat(server.passage.awaitCalls()).isEqualTo(serverAwaits);

        RequestHandle handle = client.driver.request(APPLICATION_TYPE, bytes("slow"), 100);
        settle(client, server);
        assertThat(client.passage.lastAwaitTimeoutMs()).isEqualTo(101);

        client.tick(101);
        settle(client, server);
        assertThat(handle.result()).isDone();
        assertThat(client.passage.lastAwaitTimeoutMs()).isNegative();
    }

    @Test
    void aDeadlineBelowOneMillisecondIsRejected() {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        Side client = new Side(link.first());
        startClient(client, config());

        assertThatThrownBy(() -> client.driver.request(APPLICATION_TYPE, bytes("x"), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aPingWithoutEightBytesFailsItsFutureAndKeepsTheConnection() {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        Side client = new Side(link.first());
        startClient(client, config());

        assertThat(client.driver.ping(new byte[3]))
                .failsWithin(5, TimeUnit.SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(IllegalArgumentException.class);
    }

    /** 켜면 같은 프레임이 끝없이(정해진 수만큼) 읽혀 나오는 통로. 나머지는 안쪽 통로에 맡긴다. */
    private static final class FloodingPassage implements Passage {
        private final InMemoryPassage inner;
        private final AtomicLong remaining = new AtomicLong();
        private volatile byte[] frame = new byte[0];
        private final CountDownLatch drained = new CountDownLatch(1);

        FloodingPassage(InMemoryPassage inner) {
            this.inner = inner;
        }

        void flood(byte[] frame, long count) {
            this.frame = frame;
            remaining.set(count);
            inner.wakeup();
        }

        @Override
        public Inbound await(long timeoutMs) throws IOException {
            while (true) {
                if (remaining.get() > 0) {
                    if (remaining.decrementAndGet() == 0) {
                        drained.countDown();
                    }
                    return Inbound.message(WebSocketMessageKind.BINARY, frame);
                }
                Inbound inbound = inner.await(timeoutMs);
                // 기다리는 동안 켜졌으면 깨어난 그 읽기에서 바로 쏟아낸다.
                if (inbound.kind() != Inbound.Kind.NONE || remaining.get() == 0) {
                    return inbound;
                }
            }
        }

        @Override
        public void write(OutboundMessage message) throws IOException {
            inner.write(message);
        }

        @Override
        public void wakeup() {
            inner.wakeup();
        }

        @Override
        public void close() {
            inner.close();
        }
    }

    @Test
    void aFloodOfInputDoesNotStarveTheClockAndTheRestIsReadOnTheFollowingSteps() throws Exception {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        FloodingPassage flooding = new FloodingPassage(client.passage);
        startServer(server, config());
        client.driver = ConnectionDriver.client(config(), flooding)
                .handler(handlers, request -> {})
                .clock(client.clock::get)
                .start();
        started.add(client.driver);
        settle(client, server);
        // 쏟아낼 프레임은 서버가 실제로 보낸 PING이다. 몇 번을 받아도 연결에 해가 없다.
        server.driver.ping(new byte[8]).join();
        settle(client, server);
        List<byte[]> sent = server.passage.written();
        byte[] ping = sent.get(sent.size() - 1);
        RequestHandle handle = client.driver.request(APPLICATION_TYPE, bytes("slow"), 100);
        settle(client, server);
        AtomicLong leftWhenEnded = new AtomicLong(-1);
        CompletableFuture<Void> recorded = handle.result().thenRun(() -> leftWhenEnded.set(flooding.remaining.get()));

        client.clock.addAndGet(101);
        flooding.flood(ping, FLOOD_SIZE);

        assertThat(handle.result().get(5, TimeUnit.SECONDS).status()).isEqualTo(STATUS_DEADLINE_EXCEEDED);
        // 기한이 처리된 시점에 쌓인 입력이 아직 남아 있어야 한다(받기만 하다 굶지 않았다).
        assertThat(leftWhenEnded.get()).isPositive();
        assertThat(recorded).isCompleted();
        // 남은 것도 다음 스텝들에서 이어 받는다.
        assertThat(flooding.drained.await(5, TimeUnit.SECONDS)).isTrue();
    }
}
