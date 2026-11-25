package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.ProgressSpace;
import io.github.ppzxc.strand.netty.internal.SerialExecutor;
import io.github.ppzxc.strand.netty.internal.TlsSizes;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoop;
import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;
import javax.net.ssl.SSLContext;
import org.jspecify.annotations.Nullable;

final class ConnectionParams {
    enum Role {
        CLIENT,
        SERVER
    }

    interface PreparationObserver {
        void ready(NettyStrandConnection connection);

        void failed(PreparationFailure failure);

        default void rejected(PreparationFailure failure) {
            failed(failure);
        }
    }

    static final class Tls {
        final SSLContext context;
        final TlsSizes sizes;
        final @Nullable String clientHost;
        final int clientPort;

        Tls(SSLContext context, TlsSizes sizes, @Nullable String clientHost, int clientPort) {
            this.context = context;
            this.sizes = sizes;
            this.clientHost = clientHost;
            this.clientPort = clientPort;
        }
    }

    final Role role;
    final EndpointConfig config;
    final SocketAddress remoteAddress;
    final EventLoop loop;

    final SerialExecutor executor;
    final Executor appExecutor;

    final ConnectionBudgets budgets;
    final ProgressSpace space;
    final long maxFrameSize;
    final long preparationDeadlineAtNanos;

    final long closeFinishNanos;
    final long writeQueueBytes;
    final LongSupplier nanoClock;
    final PreparationObserver observer;
    final @Nullable RequestHandler handler;
    final @Nullable ConnectionHandlerFactory factory;
    final @Nullable CredentialVerifier verifier;
    final @Nullable Tls tls;
    final ByteBufAllocator allocator;

    @Nullable
    ConnectAttemptState attempt;

    @Nullable
    PreparationBytes preparationBytes;

    volatile Runnable terminated = () -> {};
    io.netty.util.concurrent.@Nullable Promise<Void> released;

    @Nullable
    UnreleasedHandlers unreleased;

    HostResolver resolver = HostResolver.SYSTEM;
    Transport transport = Transport.tcp();

    ConnectionParams(
            Role role,
            EndpointConfig config,
            SocketAddress remoteAddress,
            EventLoop loop,
            SerialExecutor executor,
            Executor appExecutor,
            ConnectionBudgets budgets,
            ProgressSpace space,
            long maxFrameSize,
            long preparationDeadlineAtNanos,
            long closeFinishNanos,
            long writeQueueBytes,
            LongSupplier nanoClock,
            PreparationObserver observer,
            @Nullable RequestHandler handler,
            @Nullable ConnectionHandlerFactory factory,
            @Nullable CredentialVerifier verifier,
            @Nullable Tls tls,
            ByteBufAllocator allocator) {
        this.role = role;
        this.config = config;
        this.remoteAddress = remoteAddress;
        this.loop = loop;
        this.executor = executor;
        this.appExecutor = appExecutor;
        this.budgets = budgets;
        this.space = space;
        this.maxFrameSize = maxFrameSize;
        this.preparationDeadlineAtNanos = preparationDeadlineAtNanos;
        this.closeFinishNanos = closeFinishNanos;
        this.writeQueueBytes = writeQueueBytes;
        this.nanoClock = nanoClock;
        this.observer = observer;
        this.handler = handler;
        this.factory = factory;
        this.verifier = verifier;
        this.tls = tls;
        this.allocator = allocator;
    }
}
