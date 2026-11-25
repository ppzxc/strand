package io.github.ppzxc.strand.nio.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class ControlledPassage implements Passage {

    private static final int PREFACE_BYTES = 8;

    private static final int HEADER_BYTES = 20;
    private static final long UNLIMITED = Long.MAX_VALUE;
    private static final long WAIT_MS = 5000;

    private static final long SETTLE_MS = 100;

    public static final class Frame {
        private final byte[] bytes;

        private Frame(byte[] bytes) {
            this.bytes = bytes;
        }

        public byte[] bytes() {
            return bytes.clone();
        }

        public int type() {
            return ((bytes[4] & 0xff) << 8) | (bytes[5] & 0xff);
        }

        public long streamId() {
            return ByteBuffer.wrap(bytes, 8, 8).getLong();
        }

        public byte[] payload() {
            return Arrays.copyOfRange(bytes, HEADER_BYTES, bytes.length);
        }
    }

    private static final class FrameLog {
        private final List<Frame> frames = new ArrayList<>();
        private final ByteArrayOutputStream pending = new ByteArrayOutputStream();
        private long totalBytes;
        private boolean prefaceChecked;
        private int prefaceLeft;

        void append(byte[] bytes, int offset, int length) {
            totalBytes += length;
            pending.write(bytes, offset, length);
            byte[] buffered = pending.toByteArray();
            int position = 0;
            if (!prefaceChecked && buffered.length >= 2) {
                prefaceChecked = true;
                prefaceLeft = (buffered[0] & 0xff) == 0xc0 && (buffered[1] & 0xff) == 0xde ? PREFACE_BYTES : 0;
            }
            if (!prefaceChecked) {
                return;
            }
            int skipped = Math.min(prefaceLeft, buffered.length);
            prefaceLeft -= skipped;
            position += skipped;
            while (prefaceLeft == 0 && buffered.length - position >= 4) {
                int frameBytes = 4 + ByteBuffer.wrap(buffered, position, 4).getInt();
                if (buffered.length - position < frameBytes) {
                    break;
                }
                frames.add(new Frame(Arrays.copyOfRange(buffered, position, position + frameBytes)));
                position += frameBytes;
            }
            if (position > 0) {
                pending.reset();
                pending.write(buffered, position, buffered.length - position);
            }
        }
    }

    private final Passage delegate;
    private final Object lock = new Object();
    private final FrameLog written = new FrameLog();
    private final FrameLog read = new FrameLog();
    private final List<byte[]> deferred = new ArrayList<>();
    private boolean deferWrites;
    private boolean holdWakeups;
    private boolean waiting;
    private long waitEntries;
    private long writeCapacity = UNLIMITED;
    private long blockedWrites;
    private int readChunkBytes = Integer.MAX_VALUE;
    private byte[] unreadChunk = new byte[0];

    public ControlledPassage(Passage delegate) {
        this.delegate = delegate;
    }

    @Override
    public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
        if (timeoutMs == 0) {
            return recordRead(poll(waitForRead, waitForWrite));
        }
        synchronized (lock) {
            waitEntries++;
            waiting = true;
            lock.notifyAll();
        }
        try {
            return recordRead(poll(timeoutMs, waitForRead, waitForWrite));
        } finally {
            synchronized (lock) {
                waiting = false;
            }
        }
    }

    private Inbound poll(boolean waitForRead, boolean waitForWrite) throws IOException {
        return poll(0, waitForRead, waitForWrite);
    }

    private Inbound poll(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
        synchronized (lock) {
            if (waitForRead && unreadChunk.length > 0) {
                return nextChunk();
            }

            if (writeCapacity == 0) {
                waitForWrite = false;
            }
        }
        Inbound inbound = delegate.await(timeoutMs, waitForRead, waitForWrite);
        synchronized (lock) {
            if (inbound.kind() == Inbound.Kind.BYTES && inbound.data().length > readChunkBytes) {
                unreadChunk = inbound.data();
                return nextChunk();
            }
        }
        return inbound;
    }

    private Inbound nextChunk() {
        int size = Math.min(readChunkBytes, unreadChunk.length);
        byte[] chunk = Arrays.copyOfRange(unreadChunk, 0, size);
        unreadChunk = Arrays.copyOfRange(unreadChunk, size, unreadChunk.length);
        return Inbound.bytes(chunk);
    }

    private Inbound recordRead(Inbound inbound) {
        if (inbound.kind() == Inbound.Kind.BYTES || inbound.kind() == Inbound.Kind.UNIT) {
            synchronized (lock) {
                read.append(inbound.data(), 0, inbound.data().length);
            }
        }
        return inbound;
    }

    @Override
    public int writeBytes(byte[] bytes, int offset, int length) throws IOException {
        synchronized (lock) {
            if (deferWrites) {

                deferred.add(Arrays.copyOfRange(bytes, offset, offset + length));
                written.append(bytes, offset, length);
                return length;
            }
            int allowed = (int) Math.min(length, writeCapacity);
            if (allowed < length) {
                blockedWrites++;
                lock.notifyAll();
            }
            if (allowed == 0) {
                return 0;
            }
            int count = delegate.writeBytes(bytes, offset, allowed);
            if (writeCapacity != UNLIMITED) {
                writeCapacity -= count;
            }
            written.append(bytes, offset, count);
            return count;
        }
    }

    @Override
    public boolean pendingOutput() {
        return delegate.pendingOutput();
    }

    @Override
    public void wakeup() {
        synchronized (lock) {
            if (holdWakeups) {
                return;
            }
        }
        delegate.wakeup();
    }

    @Override
    public void shutdownOutput() {
        delegate.shutdownOutput();
    }

    @Override
    public void close() {
        delegate.close();
    }

    public List<Frame> writtenFrames() {
        synchronized (lock) {
            return new ArrayList<>(written.frames);
        }
    }

    public List<Frame> readFrames() {
        synchronized (lock) {
            return new ArrayList<>(read.frames);
        }
    }

    public List<Integer> writtenFrameTypes() {
        List<Integer> types = new ArrayList<>();
        for (Frame frame : writtenFrames()) {
            types.add(frame.type());
        }
        return types;
    }

    public long writtenBytes() {
        synchronized (lock) {
            return written.totalBytes;
        }
    }

    public long readBytes() {
        synchronized (lock) {
            return read.totalBytes;
        }
    }

    public void holdWakeups() throws InterruptedException {
        long enteredBefore;
        synchronized (lock) {
            holdWakeups = true;
            enteredBefore = waitEntries;
        }

        delegate.wakeup();
        long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
        synchronized (lock) {
            long stableUntil = 0;
            long stableEntries = -1;
            while (true) {
                long now = System.nanoTime();
                if (waitEntries > enteredBefore && waiting) {
                    if (stableEntries != waitEntries) {
                        stableEntries = waitEntries;
                        stableUntil = now + TimeUnit.MILLISECONDS.toNanos(SETTLE_MS);
                    } else if (now >= stableUntil) {
                        return;
                    }
                }
                if (now > limit) {
                    throw new AssertionError("전송 스레드가 " + WAIT_MS + "ms 안에 기다리는 상태가 되지 않았다");
                }
                lock.wait(
                        Math.max(1, TimeUnit.NANOSECONDS.toMillis(Math.min(limit - now, SETTLE_MS * 1_000_000L / 4))));
            }
        }
    }

    public void release() {
        synchronized (lock) {
            holdWakeups = false;
        }
        delegate.wakeup();
    }

    public void deferWrites() {
        synchronized (lock) {
            deferWrites = true;
        }
    }

    public void releaseWrites() {
        synchronized (lock) {
            deferWrites = false;
            try {
                for (byte[] bytes : deferred) {
                    int done = 0;
                    while (done < bytes.length) {
                        int accepted = delegate.writeBytes(bytes, done, bytes.length - done);
                        done += accepted;
                        if (accepted == 0) {
                            delegate.await(-1, false, true);
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            deferred.clear();
        }
    }

    public void writeCapacity(long bytes) {
        synchronized (lock) {
            writeCapacity = bytes;
        }
        delegate.wakeup();
    }

    public void addWriteCapacity(long bytes) {
        synchronized (lock) {
            if (writeCapacity != UNLIMITED) {
                writeCapacity = Math.min(UNLIMITED - 1, writeCapacity + bytes);
            }
        }
        delegate.wakeup();
    }

    public void unlimitWrites() {
        synchronized (lock) {
            writeCapacity = UNLIMITED;
        }
        delegate.wakeup();
    }

    public boolean awaitBlockedWrites(long atLeast) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
        synchronized (lock) {
            while (blockedWrites < atLeast) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(limit - System.nanoTime());
                if (remainingMs <= 0) {
                    return false;
                }
                lock.wait(remainingMs);
            }
            return true;
        }
    }

    public long blockedWrites() {
        synchronized (lock) {
            return blockedWrites;
        }
    }

    public void readChunkBytes(int bytes) {
        synchronized (lock) {
            readChunkBytes = bytes;
        }
    }
}
