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

/**
 * 클라이언트 연결 스레드가 서버 주소로 TCP 연결을 맺고 채널 보안 설정에 맞는 통로를 여는 동작. 주소 해석은 앱용 실행기의 작업으로 하고 기다림에 준비 기한을
 * 적용하며, 해석된 주소는 하나씩 순서대로 시도한다. 모든 시도는 처음 정한 준비 기한을 공유한다. 취소는 진행 중인 해석 대기와 TCP 연결을 끊는다.
 */
final class TcpConnector implements ConnectionParams.PassageOpener {
    /** 호스트 이름을 주소 목록으로 해석한다. 블로킹할 수 있다. */
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
        IOException last = null;
        for (InetSocketAddress target : targets(deadline)) {
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
        state.setInProgress(() -> PassageFactory.closeQuietly(channel));
        try {
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.socket().connect(target, (int) Math.min(remainingNanos / 1_000_000L + 1, Integer.MAX_VALUE));
        } catch (IOException | RuntimeException e) {
            PassageFactory.closeQuietly(channel);
            throw e;
        } finally {
            state.setInProgress(null);
        }
        return PassageFactory.open(channel, tlsContext, target.getHostString(), target.getPort());
    }

    /** 해석하지 않은 주소는 해석 작업 슬롯을 예약해 앱용 실행기에서 해석한다. 해석한 주소는 보관 바이트를 예약한 뒤 보관한다. */
    private List<InetSocketAddress> targets(long deadline) throws IOException {
        List<InetSocketAddress> targets = new ArrayList<>();
        if (!address.isUnresolved()) {
            targets.add(address);
            return targets;
        }
        if (!budgets.owner().tryReserve(Resource.RESOLVE_TASKS, 1)) {
            throw new IOException(new ResourceExhaustedException(Resource.RESOLVE_TASKS));
        }
        CompletableFuture<InetAddress[]> resolved = new CompletableFuture<>();
        state.setInProgress(() -> resolved.completeExceptionally(new InterruptedIOException("연결 시도가 취소됐다")));
        try {
            executor.execute(() -> {
                try {
                    resolved.complete(resolver.resolve(address.getHostString()));
                } catch (Throwable t) {
                    resolved.completeExceptionally(t);
                } finally {
                    // 해석 작업이 실제로 반환할 때까지 슬롯을 유지한다. 기한·취소가 기다림을 끝내도 반환하지 않는다.
                    budgets.owner().release(Resource.RESOLVE_TASKS, 1);
                }
            });
        } catch (RuntimeException | Error e) {
            state.setInProgress(null);
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
            state.setInProgress(null);
        }
    }
}
