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

@SuppressWarnings("EmptyCatch")
public final class Listener {
    private final ServerSocketChannel server;
    private final SocketAddress localAddress;
    private static final long RETRY_PAUSE_NANOS = 10_000_000L;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final Executor appExecutor;
    private final long fallbackNanos;

    private final CompletableFuture<Void> closedStage = new CompletableFuture<>();

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

                    LockSupport.parkNanos(RETRY_PAUSE_NANOS);
                }
            }
        } finally {

            closeServerQuietly();
            terminated.complete(null);
            complete(closedStage);
        }
    }

    private void closeServerQuietly() {
        try {
            server.close();
        } catch (IOException ignored) {

        }
    }

    private void complete(CompletableFuture<Void> stage) {
        StageCompletion.complete(appExecutor, () -> stage.complete(null), fallbackNanos);
    }

    CompletableFuture<Void> terminated() {
        return terminated;
    }

    void handleAccepted(SocketChannel accepted, Consumer<SocketChannel> acceptor) {
        if (closed.get()) {
            PassageFactory.closeQuietly(accepted);
            return;
        }
        try {
            acceptor.accept(accepted);
        } catch (RuntimeException e) {

            PassageFactory.closeQuietly(accepted);
        }
    }

    public SocketAddress localAddress() {
        return localAddress;
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeServerQuietly();
        }
    }

    public CompletionStage<Void> closed() {
        return closedStage.thenApply(done -> null);
    }
}
