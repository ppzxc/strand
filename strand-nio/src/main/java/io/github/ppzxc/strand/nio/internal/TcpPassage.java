package io.github.ppzxc.strand.nio.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("EmptyCatch")
public final class TcpPassage implements Passage {

    public static final int DEFAULT_READ_BUFFER_BYTES = 16 * 1024;

    private final SocketChannel channel;
    private final Selector selector;
    private final SelectionKey key;
    private final ByteBuffer readBuffer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private boolean ended;

    private TcpPassage(SocketChannel channel, Selector selector, SelectionKey key, int readBufferBytes) {
        this.channel = channel;
        this.selector = selector;
        this.key = key;
        this.readBuffer = ByteBuffer.allocateDirect(readBufferBytes);
    }

    public static TcpPassage of(SocketChannel channel) throws IOException {
        return of(channel, DEFAULT_READ_BUFFER_BYTES);
    }

    public static TcpPassage of(SocketChannel channel, int readBufferBytes) throws IOException {
        if (readBufferBytes < 1) {
            throw new IllegalArgumentException("readBufferBytes는 1 이상이어야 한다: " + readBufferBytes);
        }
        channel.configureBlocking(false);

        Selector selector = Selector.open();
        try {
            SelectionKey key = channel.register(selector, 0);
            return new TcpPassage(channel, selector, key, readBufferBytes);
        } catch (IOException | RuntimeException e) {
            closeQuietly(selector);
            throw e;
        }
    }

    @Override
    public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
        try {
            if (waitForRead) {
                Inbound inbound = readNow();
                if (inbound.kind() != Inbound.Kind.NONE) {
                    return inbound;
                }
            }
            if (timeoutMs == 0) {
                return Inbound.none();
            }
            int interest = (waitForRead ? SelectionKey.OP_READ : 0) | (waitForWrite ? SelectionKey.OP_WRITE : 0);
            key.interestOps(interest);
            if (timeoutMs < 0) {
                selector.select();
            } else {
                selector.select(timeoutMs);
            }
            selector.selectedKeys().clear();
            if (closed.get()) {

                throw new IOException("닫힌 통로");
            }
            return waitForRead ? readNow() : Inbound.none();
        } catch (ClosedSelectorException | CancelledKeyException e) {
            throw new IOException("닫힌 통로", e);
        }
    }

    private Inbound readNow() throws IOException {
        if (ended) {
            return Inbound.end();
        }
        readBuffer.clear();
        int count = channel.read(readBuffer);
        if (count < 0) {
            ended = true;
            return Inbound.end();
        }
        if (count == 0) {
            return Inbound.none();
        }
        readBuffer.flip();
        byte[] bytes = new byte[count];
        readBuffer.get(bytes);
        return Inbound.bytes(bytes);
    }

    @Override
    public int writeBytes(byte[] bytes, int offset, int length) throws IOException {
        if (length <= 0) {
            return 0;
        }
        try {
            return channel.write(ByteBuffer.wrap(bytes, offset, length));
        } catch (ClosedSelectorException | CancelledKeyException e) {
            throw new IOException("닫힌 통로", e);
        }
    }

    @Override
    public void wakeup() {
        try {
            selector.wakeup();
        } catch (ClosedSelectorException | CancelledKeyException e) {

        }
    }

    @Override
    public void shutdownOutput() {
        try {
            channel.shutdownOutput();
        } catch (IOException | RuntimeException e) {

        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        closeQuietly(channel);
        closeQuietly(selector);
    }

    private static void closeQuietly(java.io.Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {

        }
    }
}
