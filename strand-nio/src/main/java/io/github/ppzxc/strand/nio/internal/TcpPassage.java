package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import org.jspecify.annotations.Nullable;

/**
 * JDK NIO 기반 TCP 통로. 이미 맺어진 {@link SocketChannel} 하나를 받아 바이트 스트림 통로({@link Inbound#bytes})로 쓴다. 채널은 이 통로가
 * 논블로킹으로 바꾸고 소유하므로, 넘긴 뒤에는 직접 읽고 쓰지 않는다.
 *
 * <p>읽기는 직접 버퍼 하나를 다시 쓰고, 읽은 양만큼 잘라 새 배열로 넘긴다({@code Endpoint.receive}에 길이 인자가 없어서 이 복사 한 번은 남는다).
 * 쓰기는 부분 쓰기를 이어 쓰고, 소켓 버퍼가 차면 쓸 수 있을 때까지 기다린다(백프레셔). 서로 쓰기만 하며 읽지 않는 두 끝이 맞물려 멈추지 않도록, 쓰기를
 * 기다리는 동안 읽을 수 있는 것은 읽어 안쪽에 쌓아 두었다가 다음 {@link #await}가 돌려준다. 쌓이는 양은 그 사이 상대가 보낸 양까지다.
 * 상대의 EOF는 {@link Inbound#end()}로, 리셋 같은 입출력 오류는 {@link IOException}으로 알린다.
 *
 * <p>{@link #await}와 {@link #write}는 전송 스레드 하나만 부른다. {@link #wakeup}과 {@link #close}는 어느 스레드에서든 부른다.
 */
public final class TcpPassage implements Passage {

    /** 읽기 버퍼의 기본 크기(바이트). */
    public static final int DEFAULT_READ_BUFFER_BYTES = 16 * 1024;

    private final SocketChannel channel;
    private final Selector readSelector;
    private final ByteBuffer readBuffer;
    private final ArrayDeque<Inbound> stashed = new ArrayDeque<>();
    private final Object closeLock = new Object();
    private @Nullable Selector writeSelector;
    private @Nullable SelectionKey writeKey;
    private boolean ended;
    private volatile boolean closed;

    private TcpPassage(SocketChannel channel, Selector readSelector, int readBufferBytes) {
        this.channel = channel;
        this.readSelector = readSelector;
        this.readBuffer = ByteBuffer.allocateDirect(readBufferBytes);
    }

    /**
     * 맺어진 채널을 기본 읽기 버퍼 크기({@value #DEFAULT_READ_BUFFER_BYTES}바이트)로 통로로 만든다.
     *
     * @param channel 맺어진 소켓 채널. 이 통로가 논블로킹으로 바꾸고 소유한다
     * @return 통로
     * @throws IOException 채널을 다룰 수 없을 때
     */
    public static TcpPassage of(SocketChannel channel) throws IOException {
        return of(channel, DEFAULT_READ_BUFFER_BYTES);
    }

    /**
     * 맺어진 채널을 통로로 만든다.
     *
     * @param channel 맺어진 소켓 채널. 이 통로가 논블로킹으로 바꾸고 소유한다
     * @param readBufferBytes 한 번에 읽는 최대 바이트 수(1 이상). 한 번 읽은 것이 이보다 크게 넘어가지 않는다
     * @return 통로
     * @throws IOException 채널을 다룰 수 없을 때
     * @throws IllegalArgumentException {@code readBufferBytes}가 1보다 작을 때
     */
    public static TcpPassage of(SocketChannel channel, int readBufferBytes) throws IOException {
        if (readBufferBytes < 1) {
            throw new IllegalArgumentException("readBufferBytes는 1 이상이어야 한다: " + readBufferBytes);
        }
        channel.configureBlocking(false);
        Selector selector = Selector.open();
        try {
            channel.register(selector, SelectionKey.OP_READ);
        } catch (IOException | RuntimeException e) {
            selector.close();
            throw e;
        }
        return new TcpPassage(channel, selector, readBufferBytes);
    }

    @Override
    public Inbound await(long timeoutMs) throws IOException {
        try {
            Inbound pending = stashed.poll();
            if (pending != null) {
                return pending;
            }
            Inbound inbound = readNow();
            if (inbound.kind() != Inbound.Kind.NONE || timeoutMs == 0) {
                return inbound;
            }
            if (timeoutMs < 0) {
                readSelector.select();
            } else {
                readSelector.select(timeoutMs);
            }
            readSelector.selectedKeys().clear();
            return readNow();
        } catch (ClosedSelectorException | CancelledKeyException e) {
            throw new IOException("닫힌 통로", e);
        }
    }

    /** 기다리지 않고 지금 읽을 수 있는 것을 읽는다. */
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
    public void write(OutboundMessage message) throws IOException {
        if (message.kind() != OutboundMessage.Kind.BINARY) {
            // TCP에는 WebSocket Close에 해당하는 것이 없다.
            return;
        }
        ByteBuffer remaining = ByteBuffer.wrap(message.bytes());
        try {
            while (remaining.hasRemaining()) {
                if (channel.write(remaining) == 0) {
                    awaitWritable();
                }
            }
        } catch (ClosedSelectorException | CancelledKeyException e) {
            throw new IOException("닫힌 통로", e);
        }
    }

    /** 소켓 버퍼에 자리가 날 때까지 기다린다. 기다리는 동안 읽을 수 있는 것은 읽어 쌓아 둔다. */
    private void awaitWritable() throws IOException {
        SelectionKey key = writeKey();
        key.interestOps(ended ? SelectionKey.OP_WRITE : SelectionKey.OP_WRITE | SelectionKey.OP_READ);
        Selector selector = key.selector();
        selector.select();
        selector.selectedKeys().clear();
        if (!ended) {
            Inbound inbound = readNow();
            if (inbound.kind() != Inbound.Kind.NONE) {
                stashed.add(inbound);
            }
        }
    }

    private SelectionKey writeKey() throws IOException {
        synchronized (closeLock) {
            if (closed) {
                throw new IOException("닫힌 통로");
            }
            SelectionKey key = writeKey;
            if (key == null) {
                Selector selector = Selector.open();
                writeSelector = selector;
                key = channel.register(selector, SelectionKey.OP_WRITE);
                writeKey = key;
            }
            return key;
        }
    }

    @Override
    public void wakeup() {
        try {
            readSelector.wakeup();
        } catch (ClosedSelectorException | CancelledKeyException e) {
            // 이미 닫혔다. 기다리는 쪽은 닫힘으로 깨어난다.
        }
    }

    @Override
    public void shutdownOutput() {
        try {
            channel.shutdownOutput();
        } catch (IOException | RuntimeException e) {
            // 이미 끊겼거나 닫혔다. 보내는 쪽을 닫으려던 것이므로 같은 결과다.
        }
    }

    @Override
    public void close() {
        synchronized (closeLock) {
            if (closed) {
                return;
            }
            closed = true;
        }
        closeQuietly(channel);
        closeQuietly(readSelector);
        Selector writing = writeSelector;
        if (writing != null) {
            closeQuietly(writing);
        }
    }

    private static void closeQuietly(java.io.Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            // 닫는 중의 오류는 할 수 있는 일이 없다.
        }
    }
}
