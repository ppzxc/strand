package io.github.ppzxc.strand.nio.internal;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class InMemoryPassage implements Passage {

    private static final long UNLIMITED = Long.MAX_VALUE;

    public static final class Pair {
        private final InMemoryPassage first;
        private final InMemoryPassage second;

        private Pair(InMemoryPassage first, InMemoryPassage second) {
            this.first = first;
            this.second = second;
        }

        public InMemoryPassage first() {
            return first;
        }

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

    public static Pair streams() {
        return connected(false);
    }

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

    private boolean readable() {
        return !held && !inbox.isEmpty();
    }

    @Override
    public int writeBytes(byte[] bytes, int offset, int length) throws IOException {
        byte[] slice;
        byte[] whole = null;
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
            partial = concat(partial, slice);
            if (accepted == length) {
                whole = partial;
                partial = new byte[0];
                written.add(whole.clone());
            }
        }
        if (!messages) {
            peer.deliver(Inbound.bytes(slice));
        } else if (whole != null) {
            peer.deliver(Inbound.unit(whole));
        }
        return slice.length;
    }

    public byte[] writtenBytes() {
        synchronized (lock) {
            return writtenBytes.toByteArray();
        }
    }

    public void failWrites() {
        synchronized (lock) {
            activity++;
            failingWrites = true;
            lock.notifyAll();
        }
    }

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

    public InMemoryPassage manualTime() {
        synchronized (lock) {
            manualTime = true;
        }
        return this;
    }

    public long awaitCalls() {
        synchronized (lock) {
            return awaitCalls;
        }
    }

    public void holdDeliveries() {
        synchronized (lock) {
            held = true;
        }
    }

    public void releaseDeliveries() {
        synchronized (lock) {
            activity++;
            held = false;
            lock.notifyAll();
        }
    }

    public void writeCapacity(long bytes) {
        synchronized (lock) {
            activity++;
            writeCapacity = bytes;
            lock.notifyAll();
        }
    }

    public void addWriteCapacity(long bytes) {
        synchronized (lock) {
            activity++;
            writeCapacity = writeCapacity > UNLIMITED - bytes ? UNLIMITED : writeCapacity + bytes;
            lock.notifyAll();
        }
    }

    public void mute() {
        synchronized (lock) {
            muted = true;
        }
    }

    public long lastAwaitTimeoutMs() {
        synchronized (lock) {
            return lastAwaitTimeoutMs;
        }
    }

    public List<byte[]> written() {
        synchronized (lock) {
            return new ArrayList<>(written);
        }
    }

    private boolean parked() {
        synchronized (lock) {
            return closed
                    || (parked && !(parkedForRead && readable()) && !(parkedForWrite && writeCapacity > 0) && !woken);
        }
    }

    public boolean isClosed() {
        synchronized (lock) {
            return closed;
        }
    }

    private long activity() {
        synchronized (lock) {
            return activity;
        }
    }

    public static void awaitQuiescence(InMemoryPassage... passages) {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
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
