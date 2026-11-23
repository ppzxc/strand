package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * CLOSED 뒤 마무리(남은 쓰기, 상대가 끊을 때까지 읽기)에 시간 상한이 있는지 본다. 단조 시계를 주입하고 통로는 시간 제한이 실제 시간으로 흐르지 않는
 * {@link InMemoryPassage}다. 전송 스레드가 끝났는지는 {@code close()} 없이 본다.
 */
class ConnectionDriverClosedFinishTest {

    private static final long UNRESPONSIVE_ERROR_CODE = 6;
    private static final long ACK_TIMEOUT_MS = 1000;
    private static final long IDLE_INTERVAL_MS = 3000;
    private static final long DEFAULT_LIMIT_MS = 5000;
    /** 빌더에 상한을 주지 않는다(기본값). */
    private static final long DEFAULT = 0;

    private static final long PLENTY = 1_000_000;

    /** 상대의 끊김(END)을 전하지 않는 통로. 연결 종료(CLOSE)를 마쳐도 상대가 끊지 않는 상황을 만든다. */
    private static final class IgnoresEndPassage implements Passage {
        private final InMemoryPassage inner;

        IgnoresEndPassage(InMemoryPassage inner) {
            this.inner = inner;
        }

        @Override
        public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
            Inbound inbound = inner.await(timeoutMs, waitForRead, waitForWrite);
            return inbound.kind() == Inbound.Kind.END ? Inbound.none() : inbound;
        }

        @Override
        public int writeBytes(byte[] bytes, int offset, int length) throws IOException {
            return inner.writeBytes(bytes, offset, length);
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

    private final AtomicLong clock = new AtomicLong(1_000_000);
    private final ConcurrentLinkedQueue<ConnectionClosed> closed = new ConcurrentLinkedQueue<>();
    private final List<ConnectionDriver> started = new ArrayList<>();
    private final ExecutorService handlers = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "test-handler");
        thread.setDaemon(true);
        return thread;
    });
    private InMemoryPassage clientPassage;
    private InMemoryPassage serverPassage;
    private ConnectionDriver client;
    private ConnectionDriver server;

    @AfterEach
    void stopAll() {
        started.forEach(ConnectionDriver::close);
        handlers.shutdownNow();
    }

    private static EndpointConfig config() {
        return EndpointConfig.defaults().ackTimeoutMs(ACK_TIMEOUT_MS);
    }

    private ConnectionDriver start(boolean isClient, Passage passage, long limitMs) {
        ConnectionListener listener = new ConnectionListener() {
            @Override
            public void onClosed(ConnectionClosed event) {
                if (isClient) {
                    closed.add(event);
                }
            }
        };
        ConnectionDriver.Builder builder = isClient
                ? ConnectionDriver.client(config().idleIntervalMs(IDLE_INTERVAL_MS), passage)
                : ConnectionDriver.server(config(), passage);
        if (limitMs != DEFAULT) {
            builder.closedFinishLimitMs(limitMs);
        }
        ConnectionDriver driver =
                builder.executor(handlers).listener(listener).clock(clock::get).start();
        started.add(driver);
        return driver;
    }

    /** 핸드셰이크를 마친 한 쌍. {@code clientIgnoresEnd}면 클라이언트는 상대의 끊김을 전해 받지 못한다. */
    private void connected(boolean clientIgnoresEnd, long limitMs) {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        clientPassage = link.first().manualTime();
        serverPassage = link.second().manualTime();
        server = start(false, serverPassage, limitMs);
        client = start(true, clientIgnoresEnd ? new IgnoresEndPassage(clientPassage) : clientPassage, limitMs);
        settle();
    }

    private void settle() {
        InMemoryPassage.awaitQuiescence(clientPassage, serverPassage);
        // 종료 알림은 핸들러 실행기(하나의 스레드)에서 불리므로, 실행기에 넘어간 알림이 모두 처리되기를 기다린다.
        try {
            handlers.submit(() -> {}).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IllegalStateException("핸들러 실행기가 비지 않았다", e);
        }
    }

    private void tick(long ms) {
        clock.addAndGet(ms);
        clientPassage.wakeup();
        serverPassage.wakeup();
        settle();
    }

    private static boolean ended(ConnectionDriver driver) {
        try {
            driver.transportThread().join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return !driver.transportThread().isAlive();
    }

    @Test
    void afterClosedTheTransportThreadEndsAtTheDefaultLimitEvenIfThePeerNeverHangsUp() {
        connected(true, DEFAULT);
        client.closeConnection();
        settle();
        assertThat(closed).hasSize(1);
        assertThat(client.transportThread().isAlive()).isTrue();

        tick(DEFAULT_LIMIT_MS - 1);
        assertThat(client.transportThread().isAlive()).isTrue();

        tick(1);
        assertThat(ended(client)).isTrue();
        assertThat(closed).hasSize(1);
        assertThat(closed.peek().reason().kind()).isEqualTo(CloseReason.Kind.CLOSE);
    }

    @Test
    void theLimitCountsFromTheMomentClosedWasProducedAndNotFromWhenTheDriverStarted() {
        connected(true, DEFAULT);
        tick(DEFAULT_LIMIT_MS * 2);
        client.closeConnection();
        settle();
        tick(DEFAULT_LIMIT_MS - 1);
        assertThat(client.transportThread().isAlive()).isTrue();

        tick(1);
        assertThat(ended(client)).isTrue();
    }

    @Test
    void theLimitCanBeChanged() {
        connected(true, 200);
        client.closeConnection();
        settle();
        tick(199);
        assertThat(client.transportThread().isAlive()).isTrue();

        tick(1);
        assertThat(ended(client)).isTrue();
    }

    @Test
    void ifTheWriteStaysBlockedAfterAnErrorCloseTheSameLimitEndsTheTransportThread() {
        connected(false, DEFAULT);
        clientPassage.writeCapacity(0);
        // 막힌 쓰기 뒤라 유휴 PING을 넘기지 못한다. 보내기로 한 때부터 확인 대기 시간이 지나면 확인 대기 초과와 같게 닫는다.
        tick(IDLE_INTERVAL_MS);
        tick(ACK_TIMEOUT_MS + 1);
        assertThat(closed).hasSize(1);
        CloseReason reason = closed.peek().reason();
        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
        assertThat(reason.errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
        assertThat(client.transportThread().isAlive()).isTrue();

        tick(DEFAULT_LIMIT_MS - 1);
        assertThat(client.transportThread().isAlive()).isTrue();

        tick(1);
        assertThat(ended(client)).isTrue();
        // 이미 알린 종료 알림은 한 번뿐이고 내용도 그대로다.
        assertThat(closed).hasSize(1);
        assertThat(closed.peek().reason()).isEqualTo(reason);
    }

    @Test
    void ifThePeersCloseIsRepliedToWhileTheWriteIsBlockedTheSameLimitEndsTheTransportThread() {
        connected(true, DEFAULT);
        clientPassage.writeCapacity(0);
        server.closeConnection();
        settle();
        assertThat(closed).hasSize(1);
        assertThat(client.transportThread().isAlive()).isTrue();

        tick(DEFAULT_LIMIT_MS - 1);
        assertThat(client.transportThread().isAlive()).isTrue();

        tick(1);
        assertThat(ended(client)).isTrue();
        assertThat(closed).hasSize(1);
    }

    @Test
    void whatTheLimitCutsOffInTheWriteQueueIsCountedAsDiscardedBytes() {
        connected(true, DEFAULT);
        clientPassage.writeCapacity(0);
        java.util.concurrent.CompletableFuture<Void> unusedPing = client.ping(new byte[8]);
        // PING 명령이 전송 스레드에서 처리돼 쓰기 대기열에 놓인 뒤에 상대가 CLOSE한다(경주 방지).
        settle();
        server.closeConnection();
        settle();
        assertThat(client.discardedBytes()).isZero();

        tick(DEFAULT_LIMIT_MS);

        assertThat(ended(client)).isTrue();
        assertThat(client.discardedBytes()).as("막혀서 쓰지 못하고 버려진 바이트").isPositive();
    }

    @Test
    void nothingIsDiscardedWhenTheWriteQueueDrainsBeforeTheConnectionEnds() {
        connected(true, DEFAULT);
        client.closeConnection();
        settle();

        client.close();

        assertThat(client.discardedBytes()).isZero();
    }

    @Test
    void aVeryLargeLimitDoesNotOverflowAndTheWaitStaysWithinTheIntRange() {
        connected(true, Long.MAX_VALUE);
        client.closeConnection();
        settle();
        assertThat(closed).hasSize(1);
        assertThat(clientPassage.lastAwaitTimeoutMs()).isEqualTo(Integer.MAX_VALUE);

        tick(DEFAULT_LIMIT_MS * 10);
        assertThat(client.transportThread().isAlive()).isTrue();
        assertThat(clientPassage.lastAwaitTimeoutMs()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void theWaitAfterClosedIsBoundedByTheRemainingLimit() {
        connected(true, DEFAULT);
        client.closeConnection();
        settle();
        assertThat(clientPassage.lastAwaitTimeoutMs()).isEqualTo(DEFAULT_LIMIT_MS);

        // 시계만 가고 깨우지 않아도 다음 기다림은 남은 시간까지다.
        clock.addAndGet(1000);
        clientPassage.wakeup();
        settle();
        assertThat(clientPassage.lastAwaitTimeoutMs()).isEqualTo(DEFAULT_LIMIT_MS - 1000);
    }

    @Test
    void ifThePeerHangsUpBeforeTheLimitTheTransportThreadEndsAtOnce() {
        connected(false, DEFAULT);
        client.closeConnection();
        settle();

        assertThat(ended(client)).isTrue();
        assertThat(ended(server)).isTrue();
        assertThat(closed).hasSize(1);
    }

    @Test
    void aLimitOfZeroOrLessIsRejected() {
        InMemoryPassage passage = InMemoryPassage.messages().first();
        ConnectionDriver.Builder builder = ConnectionDriver.client(config(), passage);
        assertThatThrownBy(() -> builder.closedFinishLimitMs(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.closedFinishLimitMs(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aBlockedWriteThatClearsBeforeTheLimitStillLetsTheTransportThreadEndOnThePeersHangUp() {
        connected(false, DEFAULT);
        clientPassage.writeCapacity(0);
        // 막힌 쓰기 뒤라 유휴 PING을 넘기지 못한다. 보내기로 한 때부터 확인 대기 시간이 지나면 확인 대기 초과와 같게 닫는다.
        tick(IDLE_INTERVAL_MS);
        tick(ACK_TIMEOUT_MS + 1);
        assertThat(closed).hasSize(1);

        clientPassage.addWriteCapacity(PLENTY);
        settle();
        assertThat(ended(client)).isTrue();
        assertThat(closed).hasSize(1);
    }
}
