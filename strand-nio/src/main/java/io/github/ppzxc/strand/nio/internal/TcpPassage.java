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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JDK NIO 기반 TCP 통로. 이미 맺어진 {@link SocketChannel} 하나를 받아 바이트 스트림 통로({@link Inbound#bytes})로 쓴다. 채널은 이 통로가
 * 논블로킹으로 바꾸고 소유하므로, 넘긴 뒤에는 직접 읽고 쓰지 않는다.
 *
 * <p>읽기는 직접 버퍼 하나를 다시 쓰고, 읽은 양만큼 잘라 새 배열로 넘긴다({@code Endpoint.receive}에 길이 인자가 없어서 이 복사 한 번은 남는다).
 * 쓰기는 부분 쓰기를 이어 쓰고, 소켓 버퍼가 차면 쓸 수 있을 때까지 기다린다(백프레셔). 서로 쓰기만 하며 읽지 않는 두 끝이 맞물려 멈추지 않도록, 쓰기를
 * 기다리는 동안 읽을 수 있는 것은 읽어 안쪽에 쌓아 두었다가 다음 {@link #await}가 돌려준다. 쌓이는 양은 그 사이 상대가 보낸 양까지다.
 * 상대의 EOF는 {@link Inbound#end()}로, 리셋 같은 입출력 오류는 {@link IOException}으로 알린다. 쓰기가 실패하면 그때까지 쌓아 둔 읽기 입력은 전달되지
 * 않고 버려질 수 있다(드라이버는 쓰기 실패를 연결 끊김으로 보므로 그 입력을 더 처리하지 않는다).
 *
 * <p><b>알려진 한계.</b> 쌓아 두는 입력에는 상한이 없다. 쓰기를 기다리는 동안 상대가 보내기만 하고 우리 쪽 쓰기를 받지 않으면 메모리가 늘고, 그만큼 받는 쪽
 * 백프레셔가 사라진다. 또 쓰기에서 막혀 있는 동안 전송 스레드는 돌아오지 못하므로 시계가 가지 않는다. 확인 대기, 요청 기한, 유휴 PING이 그 사이에는
 * 동작하지 않고, 풀린 뒤에 흐른 시간이 한꺼번에 {@code advance}된다. 읽지 않는 상대에서 빠져나가는 유일한 길은 {@link ConnectionDriver#close()}(즉시
 * 끊기)다. 쌓는 양에 상한을 두려면 상한에 닿았을 때 읽기를 멈추면서도 서로 쓰기만 하는 두 끝이 교착하지 않게 하는 설계가 따로 필요하다.
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
    private final Selector writeSelector;
    private final SelectionKey writeKey;
    private final AtomicBoolean closed = new AtomicBoolean();
    private boolean ended;

    private TcpPassage(
            SocketChannel channel,
            Selector readSelector,
            Selector writeSelector,
            SelectionKey writeKey,
            int readBufferBytes) {
        this.channel = channel;
        this.readSelector = readSelector;
        this.writeSelector = writeSelector;
        this.writeKey = writeKey;
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
        // 읽기 기다림과 쓰기 기다림은 선택기를 따로 쓴다. 쓰기는 소켓 버퍼가 찼을 때만 기다리므로 쓰기 쪽 관심은 그때 켠다.
        Selector readSelector = Selector.open();
        Selector writeSelector = Selector.open();
        try {
            channel.register(readSelector, SelectionKey.OP_READ);
            SelectionKey writeKey = channel.register(writeSelector, 0);
            return new TcpPassage(channel, readSelector, writeSelector, writeKey, readBufferBytes);
        } catch (IOException | RuntimeException e) {
            closeQuietly(readSelector);
            closeQuietly(writeSelector);
            throw e;
        }
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
        // TCP에는 WebSocket Close에 해당하는 것이 없다. 그런 메시지는 내용이 비어 있어 아무것도 쓰지 않는다.
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
        writeKey.interestOps(ended ? SelectionKey.OP_WRITE : SelectionKey.OP_WRITE | SelectionKey.OP_READ);
        writeSelector.select();
        writeSelector.selectedKeys().clear();
        if (!ended) {
            Inbound inbound = readNow();
            if (inbound.kind() != Inbound.Kind.NONE) {
                stashed.add(inbound);
            }
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
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        closeQuietly(channel);
        closeQuietly(readSelector);
        closeQuietly(writeSelector);
    }

    private static void closeQuietly(java.io.Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            // 닫는 중의 오류는 할 수 있는 일이 없다.
        }
    }
}
