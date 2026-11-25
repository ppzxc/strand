package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.netty.internal.StrandServerChannel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.Nullable;

public final class Listener {
    private final StrandServerChannel channel;
    private volatile @Nullable SocketAddress localAddress;
    private final AtomicBoolean settled = new AtomicBoolean();
    private final Promise<Void> closed;
    private static final long RELEASE_POLL_NANOS = 1_000_000L;

    private final CompletableFuture<Void> terminated = new CompletableFuture<>();

    Listener(StrandServerChannel channel, EventLoop loop) {
        this.channel = channel;
        this.closed = loop.newPromise();
        closed.setUncancellable();
        channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
                super.channelUnregistered(ctx);
                ctx.executor().execute(() -> settleWhenReleased(loop));
            }
        });
    }

    void bound() {
        this.localAddress = channel.localAddress();
    }

    public SocketAddress localAddress() {
        return Objects.requireNonNull(localAddress, "바인딩하기 전에는 앱에 보이지 않는다");
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    public void close() {
        if (channel.isRegistered()) {
            channel.close();
        } else {
            channel.unsafe().closeForcibly();
            settle();
        }
    }

    public Future<Void> closed() {
        return closed;
    }

    CompletableFuture<Void> terminated() {
        return terminated;
    }

    @SuppressWarnings("EmptyCatch")
    private void settleWhenReleased(EventLoop loop) {
        if (channel.socketHeld() && !loop.isShuttingDown()) {
            try {
                loop.schedule(() -> settleWhenReleased(loop), RELEASE_POLL_NANOS, TimeUnit.NANOSECONDS)
                        .addListener(timer -> {
                            if (timer.isCancelled()) {
                                settle();
                            }
                        });
                return;
            } catch (RejectedExecutionException e) {
            }
        }
        settle();
    }

    private void settle() {
        if (settled.compareAndSet(false, true)) {
            terminated.complete(null);
            closed.trySuccess(null);
        }
    }
}
