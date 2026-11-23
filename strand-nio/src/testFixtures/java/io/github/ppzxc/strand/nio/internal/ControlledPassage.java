package io.github.ppzxc.strand.nio.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 통로를 감싸 쓰기 시점과 읽기 단위를 테스트가 정하게 하고, 오간 프레임을 기록한다. {@link PassageWrapping}으로 제품 엔진이 연 통로에 끼워 쓴다. 전송 스레드를
 * 직접 돌리지는 않는다. 전송 스레드가 {@link Passage}의 계약대로 부르는 것을 늦추거나 쪼갤 뿐이다.
 *
 * <p>하는 일: 쓴 프레임과 읽은 프레임의 기록(type, 스트림 번호, 본문), 쓰기를 모았다가 내보내기({@link #deferWrites}), 쓰기 용량 제한({@link
 * #writeCapacity}), 깨움 붙잡기({@link #holdWakeups}), 읽기를 N바이트씩 쪼개기({@link #readChunkBytes}).
 */
public final class ControlledPassage implements Passage {

    /** 연결 서문의 길이. 클라이언트가 먼저 보내며 프레임이 아니므로 기록에서 뺀다. */
    private static final int PREFACE_BYTES = 8;

    private static final int HEADER_BYTES = 20;
    private static final long UNLIMITED = Long.MAX_VALUE;
    private static final long WAIT_MS = 5000;

    /** 한쪽 방향으로 오간 프레임 하나. */
    public static final class Frame {
        private final byte[] bytes;

        private Frame(byte[] bytes) {
            this.bytes = bytes;
        }

        /**
         * 프레임 전체(헤더 포함).
         *
         * @return 프레임 바이트의 복사본
         */
        public byte[] bytes() {
            return bytes.clone();
        }

        /**
         * 프레임 type.
         *
         * @return type
         */
        public int type() {
            return ((bytes[4] & 0xff) << 8) | (bytes[5] & 0xff);
        }

        /**
         * 헤더의 스트림 번호.
         *
         * @return 스트림 번호
         */
        public long streamId() {
            return ByteBuffer.wrap(bytes, 8, 8).getLong();
        }

        /**
         * 헤더 뒤의 본문.
         *
         * @return 본문
         */
        public byte[] payload() {
            return Arrays.copyOfRange(bytes, HEADER_BYTES, bytes.length);
        }
    }

    /**
     * 바이트 흐름을 길이 필드로 프레임에 끊어 기록한다. 클라이언트가 보내는 흐름은 연결 서문 8바이트({@code 0xC0DE}로 시작)로 시작하므로 그것은 프레임으로 세지 않는다. 서버가 보내는 흐름에는
     * 서문이 없다.
     */
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
            // 쓰기 용량이 없으면 쓰기를 받아 줄 수 없는 것이므로 쓰기 가능을 기다리지 않는다. 용량이 늘면 깨운다.
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
                // 미뤄 둔 출력은 통로가 다 받은 것으로 돌려준다. 나중에 releaseWrites가 내보낸다.
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

    /**
     * 지금까지 쓴 프레임(서문 제외)을 쓴 순서대로 돌려준다. 미뤄 둔 쓰기도 쓴 것으로 센다. 일부만 쓴 프레임은 다 쓸 때까지 들어가지 않는다.
     *
     * @return 쓴 프레임
     */
    public List<Frame> writtenFrames() {
        synchronized (lock) {
            return new ArrayList<>(written.frames);
        }
    }

    /**
     * 지금까지 읽은 프레임(서문 제외)을 읽은 순서대로 돌려준다.
     *
     * @return 읽은 프레임
     */
    public List<Frame> readFrames() {
        synchronized (lock) {
            return new ArrayList<>(read.frames);
        }
    }

    /**
     * 지금까지 쓴 프레임의 type(서문 제외)을 쓴 순서대로 돌려준다.
     *
     * @return type 목록
     */
    public List<Integer> writtenFrameTypes() {
        List<Integer> types = new ArrayList<>();
        for (Frame frame : writtenFrames()) {
            types.add(frame.type());
        }
        return types;
    }

    /**
     * 통로가 받아 준 바이트 수(연결 서문 포함).
     *
     * @return 쓴 바이트 수
     */
    public long writtenBytes() {
        synchronized (lock) {
            return written.totalBytes;
        }
    }

    /**
     * 통로가 읽어 엔진에 넘긴 바이트 수(연결 서문 포함).
     *
     * @return 읽은 바이트 수
     */
    public long readBytes() {
        synchronized (lock) {
            return read.totalBytes;
        }
    }

    /**
     * 이 뒤의 {@link #wakeup}을 전송 스레드에 전하지 않고, 전송 스레드가 읽을 것 없이 기다리는 상태가 될 때까지 기다린다. 그 사이 낸 명령은 {@link
     * #release}를 부르거나 기다림이 시간 초과로 끝날 때까지 처리되지 않고 쌓였다가 한 바퀴에 함께 처리된다.
     *
     * @throws InterruptedException 기다리는 중 인터럽트됐을 때
     */
    public void holdWakeups() throws InterruptedException {
        long enteredBefore;
        synchronized (lock) {
            holdWakeups = true;
            enteredBefore = waitEntries;
        }
        // 한 바퀴를 더 돌려, 전송 스레드가 이미 처리를 끝내고 새로 기다리는 상태에 들어가게 한다.
        delegate.wakeup();
        long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
        synchronized (lock) {
            while (!(waitEntries > enteredBefore && waiting)) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(limit - System.nanoTime());
                if (remainingMs <= 0) {
                    throw new AssertionError("전송 스레드가 " + WAIT_MS + "ms 안에 기다리는 상태가 되지 않았다");
                }
                lock.wait(remainingMs);
            }
        }
    }

    /** {@link #holdWakeups}를 풀고 전송 스레드를 깨운다. */
    public void release() {
        synchronized (lock) {
            holdWakeups = false;
        }
        delegate.wakeup();
    }

    /** 이 뒤의 쓰기를 통로에 내보내지 않고 모아 둔다. {@link #releaseWrites}가 모은 것을 쓴 순서대로 내보낸다. */
    public void deferWrites() {
        synchronized (lock) {
            deferWrites = true;
        }
    }

    /** 모아 둔 쓰기를 쓴 순서대로 통로에 내보내고 이후는 바로 내보낸다. */
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

    /**
     * 통로가 앞으로 받아 줄 바이트 수를 정한다. 0이면 한 바이트도 받지 않고, 그 밖에는 그만큼만 받아 부분 쓰기가 된다. 용량을 다 쓰면 다시 정할 때까지 받지 않는다.
     *
     * @param bytes 앞으로 받아 줄 바이트 수
     */
    public void writeCapacity(long bytes) {
        synchronized (lock) {
            writeCapacity = bytes;
        }
        delegate.wakeup();
    }

    /**
     * 받아 줄 용량을 늘린다. 제한이 없는 상태면 그대로 제한 없음이다.
     *
     * @param bytes 늘릴 바이트 수
     */
    public void addWriteCapacity(long bytes) {
        synchronized (lock) {
            if (writeCapacity != UNLIMITED) {
                writeCapacity = Math.min(UNLIMITED - 1, writeCapacity + bytes);
            }
        }
        delegate.wakeup();
    }

    /** 쓰기 용량 제한을 푼다. */
    public void unlimitWrites() {
        synchronized (lock) {
            writeCapacity = UNLIMITED;
        }
        delegate.wakeup();
    }

    /**
     * 쓰기가 용량 제한 때문에 다 받아지지 못한 적이 {@code atLeast}번 이를 때까지 기다린다.
     *
     * @param atLeast 막힌 쓰기 호출 수
     * @return 상한 안에 이르렀으면 true
     * @throws InterruptedException 기다리는 중 인터럽트됐을 때
     */
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

    /**
     * 쓰기가 용량 제한 때문에 다 받아지지 못한 호출 수.
     *
     * @return 막힌 쓰기 호출 수
     */
    public long blockedWrites() {
        synchronized (lock) {
            return blockedWrites;
        }
    }

    /**
     * 한 번에 읽어 올리는 바이트를 이 크기로 쪼갠다. 프레임이 쪼개져 오는 경우를 만든다.
     *
     * @param bytes 한 번에 읽어 올리는 최대 바이트 수(1 이상)
     */
    public void readChunkBytes(int bytes) {
        synchronized (lock) {
            readChunkBytes = bytes;
        }
    }
}
