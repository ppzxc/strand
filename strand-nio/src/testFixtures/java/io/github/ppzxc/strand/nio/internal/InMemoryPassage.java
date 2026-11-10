package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * 소켓 없이 두 끝을 메모리로 이어 붙인 통로. 한쪽이 쓴 것은 다른 쪽이 읽는다. 바이트 스트림 통로({@link #streams})와 메시지 통로({@link
 * #messages}) 두 가지가 있다.
 */
public final class InMemoryPassage implements Passage {

    /** 서로 이어진 두 끝. */
    public static final class Pair {
        private final InMemoryPassage first;
        private final InMemoryPassage second;

        private Pair(InMemoryPassage first, InMemoryPassage second) {
            this.first = first;
            this.second = second;
        }

        /**
         * 한쪽 끝(보통 클라이언트가 쓴다).
         *
         * @return 첫째 끝
         */
        public InMemoryPassage first() {
            return first;
        }

        /**
         * 다른 쪽 끝(보통 서버가 쓴다).
         *
         * @return 둘째 끝
         */
        public InMemoryPassage second() {
            return second;
        }
    }

    private final boolean messages;
    private final Object lock = new Object();
    private final ArrayDeque<Inbound> inbox = new ArrayDeque<>();
    private final List<byte[]> written = new ArrayList<>();
    private boolean woken;
    private boolean closed;
    private boolean manualTime;
    private boolean parked;
    private long lastAwaitTimeoutMs = Long.MIN_VALUE;
    private long activity;
    private InMemoryPassage peer;

    private InMemoryPassage(boolean messages) {
        this.messages = messages;
    }

    private static Pair connected(boolean messages) {
        InMemoryPassage first = new InMemoryPassage(messages);
        InMemoryPassage second = new InMemoryPassage(messages);
        first.peer = second;
        second.peer = first;
        return new Pair(first, second);
    }

    /**
     * 바이트 스트림 통로 한 쌍(TCP처럼 쓴 만큼 이어 붙여 읽는다).
     *
     * @return 이어진 두 끝
     */
    public static Pair streams() {
        return connected(false);
    }

    /**
     * 메시지 통로 한 쌍(WebSocket처럼 쓴 메시지 하나가 읽는 메시지 하나다).
     *
     * @return 이어진 두 끝
     */
    public static Pair messages() {
        return connected(true);
    }

    @Override
    public Inbound await(long timeoutMs) throws IOException {
        synchronized (lock) {
            lastAwaitTimeoutMs = timeoutMs;
            activity++;
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(timeoutMs, 0));
            while (inbox.isEmpty() && !woken) {
                if (closed) {
                    throw new IOException("닫힌 통로");
                }
                try {
                    if (manualTime) {
                        // 시간 제한은 실제 시간으로 흐르지 않는다. 테스트가 시계를 올리고 wakeup으로 깨운다.
                        parked = true;
                        lock.wait();
                        parked = false;
                    } else if (timeoutMs < 0) {
                        lock.wait();
                    } else {
                        long remainingNanos = deadline - System.nanoTime();
                        if (remainingNanos <= 0) {
                            return Inbound.none();
                        }
                        TimeUnit.NANOSECONDS.timedWait(lock, remainingNanos);
                    }
                } catch (InterruptedException e) {
                    parked = false;
                    Thread.currentThread().interrupt();
                    throw new IOException("기다리다 중단됨", e);
                }
            }
            activity++;
            if (!inbox.isEmpty()) {
                return inbox.poll();
            }
            woken = false;
            return Inbound.none();
        }
    }

    @Override
    public void write(OutboundMessage message) throws IOException {
        synchronized (lock) {
            if (closed) {
                throw new IOException("닫힌 통로");
            }
        }
        if (message.kind() == OutboundMessage.Kind.BINARY) {
            byte[] bytes = message.bytes();
            synchronized (lock) {
                written.add(bytes.clone());
            }
            peer.deliver(messages ? Inbound.message(WebSocketMessageKind.BINARY, bytes) : Inbound.bytes(bytes));
        } else {
            peer.deliver(Inbound.end());
        }
    }

    @Override
    public void wakeup() {
        synchronized (lock) {
            activity++;
            woken = true;
            lock.notifyAll();
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            lock.notifyAll();
        }
        peer.deliver(Inbound.end());
    }

    private void deliver(Inbound inbound) {
        synchronized (lock) {
            activity++;
            inbox.add(inbound);
            lock.notifyAll();
        }
    }

    /**
     * 시간 제한을 실제 시간으로 흘리지 않게 한다. 제한이 있는 {@link #await}도 읽을 것이나 {@link #wakeup}이 올 때까지 기다리고, 받은 제한은
     * {@link #lastAwaitTimeoutMs}로 볼 수 있다. 드라이버를 시작하기 전에 부른다.
     *
     * @return 이 통로
     */
    public InMemoryPassage manualTime() {
        synchronized (lock) {
            manualTime = true;
        }
        return this;
    }

    /**
     * 가장 최근 {@link #await}가 받은 시간 제한(밀리초). 음수면 상한 없는 기다림이다. 아직 기다린 적이 없으면 {@link Long#MIN_VALUE}다.
     *
     * @return 최근 시간 제한
     */
    public long lastAwaitTimeoutMs() {
        synchronized (lock) {
            return lastAwaitTimeoutMs;
        }
    }

    /**
     * 이 통로로 쓴 바이트 메시지(복사본, 쓴 순서). 메시지 통로에서는 프레임 하나가 한 원소다.
     *
     * @return 쓴 메시지
     */
    public List<byte[]> written() {
        synchronized (lock) {
            return new ArrayList<>(written);
        }
    }

    /**
     * 이 통로가 읽을 것도 깨움도 없이 {@link #await}에서 기다리고 있거나 닫혔다.
     */
    private boolean parked() {
        synchronized (lock) {
            return closed || (parked && inbox.isEmpty() && !woken);
        }
    }

    private long activity() {
        synchronized (lock) {
            return activity;
        }
    }

    /**
     * 주어진 통로들이 모두 더 할 일 없이 기다리는 상태가 될 때까지 기다린다(결정적 동기화용). 드라이버들이 주고받는 쓰기가 다 처리된 뒤에야 돌아온다.
     * 실제 시간은 상한(5초)으로만 쓴다.
     *
     * @param passages 기다릴 통로들(manualTime이어야 한다)
     * @throws IllegalStateException 5초 안에 가라앉지 않을 때
     */
    public static void awaitQuiescence(InMemoryPassage... passages) {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < limit) {
            long[] before = new long[passages.length];
            boolean allParked = true;
            for (int i = 0; i < passages.length; i++) {
                before[i] = passages[i].activity();
                allParked &= passages[i].parked();
            }
            boolean stable = allParked;
            for (int i = 0; stable && i < passages.length; i++) {
                stable = passages[i].activity() == before[i];
            }
            if (stable) {
                return;
            }
            LockSupport.parkNanos(50_000L);
        }
        throw new IllegalStateException("통로가 가라앉지 않았다");
    }
}
