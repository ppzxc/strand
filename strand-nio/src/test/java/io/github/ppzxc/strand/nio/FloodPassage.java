package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.Passage;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

final class FloodPassage implements Passage {
    private Passage inner;
    private final AtomicInteger remaining = new AtomicInteger();
    private final AtomicInteger delivered = new AtomicInteger();
    private volatile IntFunction<byte[]> frames = index -> new byte[0];

    Passage around(Passage passage) {
        this.inner = passage;
        return this;
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
                return Inbound.bytes(frames.apply(delivered.getAndIncrement()));
            }
            Inbound inbound = inner.await(timeoutMs, waitForRead, waitForWrite);
            if (inbound.kind() != Inbound.Kind.NONE || !(waitForRead && remaining.get() > 0)) {
                return inbound;
            }
        }
    }

    @Override
    public int writeBytes(byte[] bytes, int offset, int length) throws IOException {
        return inner.writeBytes(bytes, offset, length);
    }

    @Override
    public boolean pendingOutput() {
        return inner.pendingOutput();
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
