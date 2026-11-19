package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * 소켓 없이 두 끝을 메모리로 이어 붙인 통로. 한쪽이 쓴 것은 다른 쪽이 읽는다. 바이트 스트림 통로({@link #streams})와 메시지 통로({@link
 * #messages}) 두 가지가 있다.
 */
public final class InMemoryPassage implements Passage {

    private static final long UNLIMITED = Long.MAX_VALUE;

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
    private boolean outputShutdown;
    private boolean manualTime;
    private boolean muted;
    private boolean held;
    private long writeCapacity = UNLIMITED;
    private boolean failingWrites;
    private final java.io.ByteArrayOutputStream writtenBytes = new java.io.ByteArrayOutputStream();
    private byte[] partial = new byte[0];
    private boolean parkedForRead;
    private boolean parkedForWrite;
    private boolean parked;
    private long lastAwaitTimeoutMs = Long.MIN_VALUE;
    private long activity;
    private long awaitCalls;
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
    public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
        synchronized (lock) {
            awaitCalls++;
            if (timeoutMs != 0) {
                lastAwaitTimeoutMs = timeoutMs;
            }
            activity++;
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(timeoutMs, 0));
            while (!(waitForRead && readable()) && !(waitForWrite && writeCapacity > 0) && !woken) {
                if (closed) {
                    throw new IOException("닫힌 통로");
                }
                try {
                    if (manualTime && timeoutMs == 0) {
                        return Inbound.none();
                    } else if (manualTime) {
                        // 시간 제한은 실제 시간으로 흐르지 않는다. 테스트가 시계를 올리고 wakeup으로 깨운다.
                        parked = true;
                        parkedForRead = waitForRead;
                        parkedForWrite = waitForWrite;
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
            if (waitForRead && readable()) {
                return inbox.poll();
            }
            woken = false;
            return Inbound.none();
        }
    }

    /** 락을 쥔 채 부른다. */
    private boolean readable() {
        return !held && !inbox.isEmpty();
    }

    @Override
    public int write(OutboundMessage message, int offset) throws IOException {
        synchronized (lock) {
            if (closed || outputShutdown) {
                throw new IOException("닫힌 통로");
            }
        }
        if (message.kind() != OutboundMessage.Kind.BINARY) {
            synchronized (lock) {
                if (muted) {
                    return 0;
                }
            }
            peer.deliver(Inbound.end());
            return 0;
        }
        int remaining = message.length() - offset;
        int accepted;
        byte[] slice;
        byte[] whole = null;
        synchronized (lock) {
            if (muted) {
                return remaining;
            }
            accepted = (int) Math.min(remaining, writeCapacity);
            if (accepted == 0) {
                return 0;
            }
            if (writeCapacity != UNLIMITED) {
                writeCapacity -= accepted;
            }
            slice = Arrays.copyOfRange(message.bytes(), offset, offset + accepted);
            partial = concat(partial, slice);
            if (accepted == remaining) {
                whole = partial;
                partial = new byte[0];
                written.add(whole.clone());
            }
        }
        if (!messages) {
            peer.deliver(Inbound.bytes(slice));
        } else if (whole != null) {
            peer.deliver(Inbound.message(WebSocketMessageKind.BINARY, whole));
        }
        return accepted;
    }

    /**
     * 진행 보고 방식 쓰기. 받아 주는 만큼({@link #writeCapacity})만 상대에게 보내고 그 바이트 수를 돌려준다. 0은 아무것도 보내지 않은 것이다. 바이트
     * 스트림 통로에서만 쓴다.
     */
    @Override
    public int writeBytes(byte[] bytes, int offset, int length) throws IOException {
        byte[] slice;
        synchronized (lock) {
            if (closed || outputShutdown) {
                throw new IOException("닫힌 통로");
            }
            if (failingWrites) {
                throw new IOException("쓰기 실패");
            }
            if (muted) {
                return length;
            }
            int accepted = (int) Math.min(length, writeCapacity);
            if (accepted == 0) {
                return 0;
            }
            if (writeCapacity != UNLIMITED) {
                writeCapacity -= accepted;
            }
            slice = Arrays.copyOfRange(bytes, offset, offset + accepted);
            writtenBytes.write(slice, 0, slice.length);
        }
        peer.deliver(Inbound.bytes(slice));
        return slice.length;
    }

    /**
     * 진행 보고 방식으로 지금까지 받아 준 바이트 전체(쓴 순서).
     *
     * @return 받아 준 바이트
     */
    public byte[] writtenBytes() {
        synchronized (lock) {
            return writtenBytes.toByteArray();
        }
    }

    /** 이 통로의 이후 쓰기가 입출력 오류로 실패하게 한다(읽기는 영향이 없다). */
    public void failWrites() {
        synchronized (lock) {
            activity++;
            failingWrites = true;
            lock.notifyAll();
        }
    }

    /**
     * 상대가 이 통로로 보낸 것처럼 바이트를 읽을 쪽 받은 편지함에 넣는다. 테스트가 프레임을 직접 만들어 보낸다.
     *
     * @param bytes 읽게 할 바이트
     */
    public void inject(byte[] bytes) {
        deliver(Inbound.bytes(bytes.clone()));
    }

    private static byte[] concat(byte[] head, byte[] tail) {
        byte[] all = Arrays.copyOf(head, head.length + tail.length);
        System.arraycopy(tail, 0, all, head.length, tail.length);
        return all;
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
    public void shutdownOutput() {
        synchronized (lock) {
            if (closed || outputShutdown) {
                return;
            }
            outputShutdown = true;
        }
        peer.deliver(Inbound.end());
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
     * {@link #await}를 부른 횟수(시간 제한 0 포함). 시계가 가지 않는데 늘면 드라이버가 되풀이해 깨어나는 것이다.
     *
     * @return 부른 횟수
     */
    public long awaitCalls() {
        synchronized (lock) {
            return awaitCalls;
        }
    }

    /**
     * 상대가 보낸 것을 받아 두기만 하고 {@link #releaseDeliveries}까지 읽게 하지 않는다. 상대가 한 스텝에 나눠 쓴 여러 프레임이 읽는 쪽에 같은
     * 읽기로 함께 도착하는 상황을 결정적으로 만든다(읽는 스레드가 첫 프레임만 보고 깨어나는 경쟁이 없다). 드라이버를 시작하기 전에 부른다.
     */
    public void holdDeliveries() {
        synchronized (lock) {
            held = true;
        }
    }

    /** {@link #holdDeliveries}로 받아 둔 것을 한꺼번에 읽게 하고 기다리는 읽기를 깨운다. */
    public void releaseDeliveries() {
        synchronized (lock) {
            activity++;
            held = false;
            lock.notifyAll();
        }
    }

    /**
     * 이 통로가 지금부터 받아 줄 수 있는 바이트 수를 정한다. 쓰기는 이만큼까지만 받고 0을 돌려주며, 받은 만큼 줄어든다. 늘리려면 {@link
     * #addWriteCapacity}를 쓴다. 정하지 않으면 한도가 없다. 쓰기 막힘을 결정적으로 만든다.
     *
     * @param bytes 받아 줄 수 있는 바이트 수(0이면 아무것도 받지 않는다)
     */
    public void writeCapacity(long bytes) {
        synchronized (lock) {
            activity++;
            writeCapacity = bytes;
            lock.notifyAll();
        }
    }

    /**
     * 받아 줄 수 있는 바이트 수를 늘리고 쓰기 가능을 기다리는 {@link #await}를 깨운다.
     *
     * @param bytes 더 받아 줄 바이트 수
     */
    public void addWriteCapacity(long bytes) {
        synchronized (lock) {
            activity++;
            writeCapacity = writeCapacity > UNLIMITED - bytes ? UNLIMITED : writeCapacity + bytes;
            lock.notifyAll();
        }
    }

    /** 이 통로로 쓰는 것을 상대에게 보내지 않고 버린다(쓰기는 성공한 것처럼 돌아온다). 말 없는 상대를 만든다. */
    public void mute() {
        synchronized (lock) {
            muted = true;
        }
    }

    /**
     * 가장 최근 {@link #await}가 받은 시간 제한(밀리초). 0은 기다리지 않고 읽을 것만 가져오는 호출이라 세지 않는다. 음수면 상한 없는 기다림이다. 아직 기다린 적이 없으면 {@link Long#MIN_VALUE}다.
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
            return closed
                    || (parked && !(parkedForRead && readable()) && !(parkedForWrite && writeCapacity > 0) && !woken);
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
