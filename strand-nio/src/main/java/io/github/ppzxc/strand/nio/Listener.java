package io.github.ppzxc.strand.nio;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * 서버의 리스닝. {@link #close}는 새 연결 수락만 멈춘다. 이미 맺어 등록한 준비 중·활성 연결은 유지한다. {@link #closed}는 리스닝 소켓과 닫기에 경합한 수락 소켓의
 * 정리가 끝나면 완료하며 기존 연결의 종료는 기다리지 않는다.
 */
public final class Listener {
    private final ServerSocketChannel server;
    private final SocketAddress localAddress;
    private static final long RETRY_PAUSE_NANOS = 10_000_000L;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final Executor appExecutor;
    private final long fallbackNanos;
    /** 앱이 관찰하는 정리 완료. 앱용 실행기를 거쳐 완료한다. */
    private final CompletableFuture<Void> closedStage = new CompletableFuture<>();
    /** 소유 객체가 내부 기록용으로 보는 정리 완료. 수락 스레드가 끝날 때 곧바로 완료한다. */
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();

    Listener(
            ServerSocketChannel server,
            SocketAddress localAddress,
            Consumer<SocketChannel> acceptor,
            Executor appExecutor,
            long fallbackNanos) {
        this.server = server;
        this.localAddress = localAddress;
        this.appExecutor = appExecutor;
        this.fallbackNanos = fallbackNanos;
        Thread thread = new Thread(() -> acceptLoop(acceptor), "strand-nio-accept");
        thread.setDaemon(true);
        thread.start();
    }

    private void acceptLoop(Consumer<SocketChannel> acceptor) {
        try {
            while (!closed.get()) {
                try {
                    handleAccepted(server.accept(), acceptor);
                } catch (ClosedChannelException e) {
                    return;
                } catch (IOException e) {
                    // 일시적인 수락 실패(예: 파일 기술자 부족)는 잠시 쉬고 다음 수락을 시도한다.
                    LockSupport.parkNanos(RETRY_PAUSE_NANOS);
                }
            }
        } finally {
            // 수락 스레드가 끝났으니 경합해 수락한 소켓은 등록됐거나 정리됐다. 리스닝 소켓도 닫는다(닫기 없이 스레드가 끝날 수 없지만 방어한다).
            closeServerQuietly();
            terminated.complete(null);
            complete(closedStage);
        }
    }

    private void closeServerQuietly() {
        try {
            server.close();
        } catch (IOException ignored) {
            // 닫는 중의 오류는 할 수 있는 일이 없다.
        }
    }

    /** 앱이 붙인 의존 단계가 수락 스레드를 막지 않게 앱용 실행기에서 완료한다. 실행기가 거절하거나 기한이 지나도록 실행하지 않으면 직접 완료한다. */
    private void complete(CompletableFuture<Void> stage) {
        StageCompletion.complete(appExecutor, () -> stage.complete(null), fallbackNanos);
    }

    /** 소유 객체가 리스닝 정리 완료를 기록하는 내부 관찰. */
    CompletableFuture<Void> terminated() {
        return terminated;
    }

    /** 수락한 소켓을 등록한다. 닫기와 경합해 수락된 소켓은 등록하지 않고 정리한다. */
    void handleAccepted(SocketChannel accepted, Consumer<SocketChannel> acceptor) {
        if (closed.get()) {
            PassageFactory.closeQuietly(accepted);
            return;
        }
        try {
            acceptor.accept(accepted);
        } catch (RuntimeException e) {
            // 한 연결의 등록 실패가 수락 스레드를 끝내지 않게 한다. 등록하지 못한 소켓은 정리한다.
            PassageFactory.closeQuietly(accepted);
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

    /** 새 연결 수락을 멈추고 리스닝 소켓을 닫는다. 이미 등록한 연결은 건드리지 않는다. 반복해도 추가 효과가 없다. */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeServerQuietly();
        }
    }

    /**
     * 리스닝 소켓과 닫기에 경합해 수락한 소켓의 정리가 끝나면 완료하는 보존된 결과. 닫기 전에는 완료하지 않는다. 이미 등록된 준비 중·활성 연결의 종료는 기다리지 않는다.
     *
     * @return 정리 완료
     */
    public CompletionStage<Void> closed() {
        return closedStage.thenApply(done -> null);
    }
}
