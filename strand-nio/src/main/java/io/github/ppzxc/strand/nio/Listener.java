package io.github.ppzxc.strand.nio;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 서버의 리스닝. {@link #close}는 새 연결 수락만 멈춘다. 이미 맺어 등록한 준비 중·활성 연결은 유지한다.
 */
public final class Listener {
    private final ServerSocketChannel server;
    private final SocketAddress localAddress;
    private final AtomicBoolean closed = new AtomicBoolean();

    Listener(ServerSocketChannel server, SocketAddress localAddress, Consumer<SocketChannel> acceptor) {
        this.server = server;
        this.localAddress = localAddress;
        Thread thread = new Thread(() -> acceptLoop(acceptor), "strand-nio-accept");
        thread.setDaemon(true);
        thread.start();
    }

    private void acceptLoop(Consumer<SocketChannel> acceptor) {
        while (!closed.get()) {
            SocketChannel accepted;
            try {
                accepted = server.accept();
            } catch (ClosedChannelException e) {
                return;
            } catch (IOException e) {
                // 일시적인 수락 실패(예: 파일 기술자 부족)는 다음 수락을 시도한다. 닫혔으면 끝낸다.
                if (closed.get() || !server.isOpen()) {
                    return;
                }
                continue;
            }
            if (accepted == null) {
                continue;
            }
            if (closed.get()) {
                // 닫기와 경합해 수락된 소켓은 정리한다.
                PassageFactory.closeQuietly(accepted);
                return;
            }
            acceptor.accept(accepted);
        }
    }

    /**
     * 바인딩된 로컬 주소.
     *
     * @return 로컬 주소
     */
    public SocketAddress localAddress() {
        return localAddress;
    }

    /** 새 연결 수락을 멈추고 리스닝 소켓을 닫는다. 반복해도 추가 효과가 없다. */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                server.close();
            } catch (IOException ignored) {
                // 닫는 중의 오류는 할 수 있는 일이 없다.
            }
        }
    }
}
