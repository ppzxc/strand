package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.PassageWrapping;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

final class ConnectionParams {
    enum Role {
        CLIENT,
        SERVER
    }

    interface PassageOpener {

        Passage open(long remainingNanos) throws IOException;
    }

    interface PreparationObserver {
        void ready(NioStrandConnection connection);

        void failed(PreparationFailure failure);

        default void rejected(PreparationFailure failure) {
            failed(failure);
        }
    }

    final Role role;
    final EndpointConfig config;
    final PassageOpener opener;
    final SocketAddress remoteAddress;
    final SerialExecutor executor;

    final java.util.concurrent.Executor appExecutor;

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
    final boolean tls;

    @Nullable
    ConnectAttemptState attempt;

    @Nullable
    UnreleasedHandlers unreleased;

    @Nullable
    PreparationBytes preparationBytes;

    PassageWrapping.@Nullable Wrapper passageWrapper;

    Runnable terminated = () -> {};

    java.util.concurrent.@Nullable CompletableFuture<Void> released;

    long releasedFallbackNanos = Long.MAX_VALUE;

    ConnectionParams(
            Role role,
            EndpointConfig config,
            PassageOpener opener,
            SocketAddress remoteAddress,
            SerialExecutor executor,
            java.util.concurrent.Executor appExecutor,
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
            boolean tls) {
        this.role = role;
        this.config = config;
        this.opener = opener;
        this.remoteAddress = remoteAddress;
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
    }
}
