package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.Passage;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import javax.net.ssl.SSLContext;
import org.jspecify.annotations.Nullable;

final class TcpConnector implements ConnectionParams.PassageOpener {

    @FunctionalInterface
    interface Resolver {
        InetAddress[] resolve(String host) throws UnknownHostException;
    }

    private final ConnectionBudgets budgets;
    private final InetSocketAddress address;
    private final @Nullable SSLContext tlsContext;
    private final Resolver resolver;
    private final Executor executor;
    private final ConnectAttemptState state;
    private final PreparationBytes preparationBytes;
    private final LongSupplier nanoClock;

    TcpConnector(
            ConnectionBudgets budgets,
            InetSocketAddress address,
            @Nullable SSLContext tlsContext,
            Resolver resolver,
            Executor executor,
            ConnectAttemptState state,
            PreparationBytes preparationBytes,
            LongSupplier nanoClock) {
        this.budgets = budgets;
        this.address = address;
        this.tlsContext = tlsContext;
        this.resolver = resolver;
        this.executor = executor;
        this.state = state;
        this.preparationBytes = preparationBytes;
        this.nanoClock = nanoClock;
    }

    @Override
    public Passage open(long remainingNanos) throws IOException {
        long deadline = Saturating.add(nanoClock.getAsLong(), remainingNanos);
        long[] resolvedBytes = new long[1];
        try {
            return open(deadline, resolvedBytes);
        } finally {

            preparationBytes.release(resolvedBytes[0]);
        }
    }

    private Passage open(long deadline, long[] resolvedBytes) throws IOException {
        IOException last = null;
        for (InetSocketAddress target : targets(deadline, resolvedBytes)) {
            long remaining = deadline - nanoClock.getAsLong();
            if (remaining <= 0) {
                throw new SocketTimeoutException("준비 기한 안에 연결하지 못했다");
            }
            try {
                return connect(target, remaining);
            } catch (IOException e) {
                if (state.cancelRequested()) {
                    throw e;
                }
                last = e;
            }
        }
        throw last != null ? last : new UnknownHostException(address.getHostString());
    }

    private Passage connect(InetSocketAddress target, long remainingNanos) throws IOException {
        SocketChannel channel = SocketChannel.open();
        state.registerAbort(() -> PassageFactory.closeQuietly(channel));
        try {
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.socket().connect(target, (int) Math.min(remainingNanos / 1_000_000L + 1, Integer.MAX_VALUE));
        } catch (IOException | RuntimeException e) {
            PassageFactory.closeQuietly(channel);
            throw e;
        } finally {
            state.registerAbort(null);
        }
        return PassageFactory.open(channel, tlsContext, target.getHostString(), target.getPort());
    }

    private List<InetSocketAddress> targets(long deadline, long[] resolvedBytes) throws IOException {
        List<InetSocketAddress> targets = new ArrayList<>();
        if (!address.isUnresolved()) {
            targets.add(address);
            return targets;
        }
        if (!budgets.owner().tryReserve(Resource.RESOLVE_TASKS, 1)) {
            throw new IOException(new ResourceExhaustedException(Resource.RESOLVE_TASKS));
        }
        CompletableFuture<InetAddress[]> resolved = new CompletableFuture<>();
        state.registerAbort(() -> resolved.completeExceptionally(new InterruptedIOException("연결 시도가 취소됐다")));
        try {
            executor.execute(() -> {
                InetAddress[] found = null;
                Throwable failure = null;
                try {
                    found = resolver.resolve(address.getHostString());
                } catch (Throwable t) {
                    failure = t;
                }

                budgets.owner().release(Resource.RESOLVE_TASKS, 1);
                if (failure != null) {
                    resolved.completeExceptionally(failure);
                } else {
                    resolved.complete(found);
                }
            });
        } catch (RuntimeException | Error e) {
            state.registerAbort(null);
            budgets.owner().release(Resource.RESOLVE_TASKS, 1);
            throw e;
        }
        InetAddress[] addresses = await(resolved, deadline);
        long bytes = 0;
        for (InetAddress each : addresses) {
            bytes += each.getAddress().length;
        }
        if (!preparationBytes.tryReserve(bytes)) {
            throw new ResourceExhaustedException(Resource.PREPARATION_BYTES);
        }
        resolvedBytes[0] = bytes;
        for (InetAddress each : addresses) {
            targets.add(new InetSocketAddress(each, address.getPort()));
        }
        return targets;
    }

    private InetAddress[] await(CompletableFuture<InetAddress[]> resolved, long deadline) throws IOException {
        try {
            long remaining = deadline - nanoClock.getAsLong();
            if (remaining <= 0) {
                throw new TimeoutException();
            }
            return resolved.get(remaining, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw new SocketTimeoutException("준비 기한 안에 연결하지 못했다");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("연결 시도가 중단됐다");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IOException(cause);
        } finally {
            state.registerAbort(null);
        }
    }
}
