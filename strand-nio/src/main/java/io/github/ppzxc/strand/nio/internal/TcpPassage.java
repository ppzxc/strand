package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.Nullable;

/**
 * JDK NIO 기반 TCP 통로. 이미 맺어진 {@link SocketChannel} 하나를 받아 바이트 스트림 통로({@link Inbound#bytes})로 쓴다. 채널은 이 통로가
 * 논블로킹으로 바꾸고 소유하므로, 넘긴 뒤에는 직접 읽고 쓰지 않는다.
 *
 * <p>읽기는 직접 버퍼 하나를 다시 쓰고, 읽은 양만큼 잘라 새 배열로 넘긴다({@code Endpoint.receive}에 길이 인자가 없어서 이 복사 한 번은 남는다).
 * 읽은 입력은 안쪽에 쌓아 두지 않고 늘 {@link #await}가 바로 돌려주므로 입력이 통로 안에서 늘지 않는다. 쓰기는 비블로킹·부분 쓰기라서 소켓 버퍼가
 * 찼으면 0을 돌려주고, 호출자(연결 드라이버)가 쓰기 대기로 기다리면서도 읽기와 시계를 계속 돌린다. 그래서 서로 쓰기만 하며 읽지 않는 두 끝도 맞물려
 * 멈추지 않는다. 읽기와 쓰기 관심은 하나의 선택기에서 다룬다. 상대의 EOF는 {@link Inbound#end()}로, 리셋 같은 입출력 오류는
 * {@link IOException}으로 알린다. 서로 쓰기만 하는 두 끝이 멈추지 않는다는 것은 정상 흐름 제어 범위 안에서만 참이다. 드라이버의 쓰기 대기열이 상한을 넘으면 드라이버가 읽기를 멈추고, 그 뒤는 유휴 PING 확인 대기 초과로 끊긴다({@code ConnectionDriver} 클래스 설명의 "쓰기 대기열 상한과 읽기 멈춤" 참고).
 *
 * <p>{@link #await}와 {@link #write}는 전송 스레드 하나만 부른다. {@link #wakeup}과 {@link #close}는 어느 스레드에서든 부른다.
 */
public final class TcpPassage implements Passage {

    /** 읽기 버퍼의 기본 크기(바이트). */
    public static final int DEFAULT_READ_BUFFER_BYTES = 16 * 1024;

    private final SocketChannel channel;
    private final Selector selector;
    private final SelectionKey key;
    private final ByteBuffer readBuffer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private boolean ended;
    private @Nullable OutboundMessage writing;
    private @Nullable ByteBuffer writingBuffer;

    private TcpPassage(SocketChannel channel, Selector selector, SelectionKey key, int readBufferBytes) {
        this.channel = channel;
        this.selector = selector;
        this.key = key;
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
        // 읽기와 쓰기 관심은 한 선택기에서 다룬다. 관심은 기다릴 때마다 정한다.
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
            int interest =
                    (waitForRead && !ended ? SelectionKey.OP_READ : 0) | (waitForWrite ? SelectionKey.OP_WRITE : 0);
            key.interestOps(interest);
            if (timeoutMs < 0) {
                selector.select();
            } else {
                selector.select(timeoutMs);
            }
            selector.selectedKeys().clear();
            if (closed.get()) {
                // 닫힘이 선택기를 깨웠다. 닫는 중에는 선택기가 아직 열려 있어 위 호출이 던지지 않을 수 있다.
                throw new IOException("닫힌 통로");
            }
            return waitForRead ? readNow() : Inbound.none();
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
    public int write(OutboundMessage message, int offset) throws IOException {
        // TCP에는 WebSocket Close에 해당하는 것이 없다. 그런 메시지는 내용이 비어 있어 아무것도 쓰지 않는다.
        if (offset >= message.length()) {
            return 0;
        }
        // bytes()는 복사본을 주므로 한 메시지를 나눠 쓰는 동안은 한 번만 복사해 둔다.
        ByteBuffer buffer = writingBuffer;
        // 복사본은 같은 메시지를 이어 쓸 때만 쓴다(위치가 offset과 같을 때). 위치가 다르면 버려진 복사본이라 새로 만든다.
        if (writing != message || buffer == null || buffer.position() != offset) {
            buffer = ByteBuffer.wrap(message.bytes());
            writing = message;
            writingBuffer = buffer;
        }
        buffer.position(offset);
        int count;
        try {
            count = channel.write(buffer);
        } catch (IOException | RuntimeException e) {
            // 실패한 메시지의 쓰기 상태는 버린다. 같은 메시지로 다시 써도 처음부터 복사한다.
            writing = null;
            writingBuffer = null;
            throw e;
        }
        if (offset + count >= message.length()) {
            writing = null;
            writingBuffer = null;
        }
        return count;
    }

    @Override
    public void wakeup() {
        try {
            selector.wakeup();
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
        closeQuietly(selector);
    }

    private static void closeQuietly(java.io.Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            // 닫는 중의 오류는 할 수 있는 일이 없다.
        }
    }
}
