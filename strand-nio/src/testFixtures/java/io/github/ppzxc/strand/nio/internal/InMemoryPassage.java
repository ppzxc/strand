package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

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
    private boolean woken;
    private boolean closed;
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
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(timeoutMs, 0));
            while (inbox.isEmpty() && !woken) {
                if (closed) {
                    throw new IOException("닫힌 통로");
                }
                try {
                    if (timeoutMs < 0) {
                        lock.wait();
                    } else {
                        long remainingNanos = deadline - System.nanoTime();
                        if (remainingNanos <= 0) {
                            return Inbound.none();
                        }
                        TimeUnit.NANOSECONDS.timedWait(lock, remainingNanos);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("기다리다 중단됨", e);
                }
            }
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
            peer.deliver(messages ? Inbound.message(WebSocketMessageKind.BINARY, bytes) : Inbound.bytes(bytes));
        } else {
            peer.deliver(Inbound.end());
        }
    }

    @Override
    public void wakeup() {
        synchronized (lock) {
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
            inbox.add(inbound);
            lock.notifyAll();
        }
    }
}
