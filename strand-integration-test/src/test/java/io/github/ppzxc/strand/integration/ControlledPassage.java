package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.PassageContract;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;

/**
 * 통로를 감싸 두 가지를 해 준다. 쓴 프레임의 type을 쓴 순서대로 적고, 전송 스레드가 같은 한 바퀴에 여러 명령을 처리하거나 두 연결이 서로의 쓰기를 보기 전에
 * 각자 쓰게 하는 시점을 테스트가 정할 수 있게 한다. 전송 스레드를 직접 돌리지는 않는다. 전송 스레드가 {@link Passage}의 계약대로 부르는 것을 늦출 뿐이다.
 */
final class ControlledPassage implements Passage {

    private static final int FRAME_HEADER_BYTES = 20;

    private final Passage delegate;
    private final Object lock = new Object();
    private final List<Integer> writtenFrameTypes = new ArrayList<>();
    private final List<OutboundMessage> deferred = new ArrayList<>();
    private @Nullable OutboundMessage lastRecorded;
    private boolean deferWrites;
    private boolean holdWakeups;
    private boolean waiting;
    private long waitEntries;

    ControlledPassage(Passage delegate) {
        this.delegate = delegate;
    }

    @Override
    public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
        if (timeoutMs == 0) {
            return delegate.await(0, waitForRead, waitForWrite);
        }
        synchronized (lock) {
            waitEntries++;
            waiting = true;
            lock.notifyAll();
        }
        try {
            return delegate.await(timeoutMs, waitForRead, waitForWrite);
        } finally {
            synchronized (lock) {
                waiting = false;
            }
        }
    }

    @Override
    public int write(OutboundMessage message, int offset) throws IOException {
        synchronized (lock) {
            // 막혀서 같은 메시지로 다시 부르는 호출은 한 번만 적는다.
            if (message != lastRecorded
                    && message.kind() == OutboundMessage.Kind.BINARY
                    && message.length() >= FRAME_HEADER_BYTES) {
                byte[] bytes = message.bytes();
                writtenFrameTypes.add(((bytes[4] & 0xff) << 8) | (bytes[5] & 0xff));
                lastRecorded = message;
            }
            if (deferWrites) {
                // 미뤄 둔 메시지는 통로가 다 받은 것으로 돌려준다. 나중에 releaseWrites가 내보낸다. 드라이버는 메시지를 순서대로 쓰므로 마지막에 모은 것과 같은 신원인지만 본다.
                if (deferred.isEmpty() || deferred.get(deferred.size() - 1) != message) {
                    deferred.add(message);
                }
                return message.length() - offset;
            }
            return delegate.write(message, offset);
        }
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

    /** 지금까지 쓴 프레임(서문 제외)의 type을 쓴 순서대로 돌려준다. 미뤄 둔 쓰기도 쓴 것으로 센다. */
    List<Integer> writtenFrameTypes() {
        synchronized (lock) {
            return new ArrayList<>(writtenFrameTypes);
        }
    }

    /**
     * 이 뒤의 {@link #wakeup}을 전송 스레드에 전하지 않고, 전송 스레드가 읽을 것 없이 기다리는 상태가 될 때까지 기다린다. 그 사이 낸 명령은 {@link
     * #release}를 부를 때까지 처리되지 않고 쌓였다가 한 바퀴에 함께 처리된다.
     */
    void holdWakeups() throws InterruptedException {
        long enteredBefore;
        synchronized (lock) {
            holdWakeups = true;
            enteredBefore = waitEntries;
        }
        // 한 바퀴를 더 돌려, 전송 스레드가 이미 처리를 끝내고 새로 기다리는 상태에 들어가게 한다.
        delegate.wakeup();
        long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Fixtures.TIMEOUT_MS);
        synchronized (lock) {
            while (!(waitEntries > enteredBefore && waiting)) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(limit - System.nanoTime());
                if (remainingMs <= 0) {
                    throw new AssertionError("전송 스레드가 " + Fixtures.TIMEOUT_MS + "ms 안에 기다리는 상태가 되지 않았다");
                }
                lock.wait(remainingMs);
            }
        }
    }

    /** {@link #holdWakeups}를 풀고 전송 스레드를 깨운다. */
    void release() {
        synchronized (lock) {
            holdWakeups = false;
        }
        delegate.wakeup();
    }

    /** 이 뒤의 쓰기를 통로에 내보내지 않고 모아 둔다. {@link #releaseWrites}가 모은 것을 쓴 순서대로 내보낸다. */
    void deferWrites() {
        synchronized (lock) {
            deferWrites = true;
        }
    }

    /** 모아 둔 쓰기를 쓴 순서대로 통로에 내보내고 이후는 바로 내보낸다. */
    void releaseWrites() {
        synchronized (lock) {
            deferWrites = false;
            try {
                for (OutboundMessage message : deferred) {
                    PassageContract.writeFully(delegate, message);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            deferred.clear();
        }
    }
}
