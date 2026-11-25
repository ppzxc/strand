package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ShutdownResult;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

@SuppressWarnings("EmptyCatch")
public final class NioStrandServer {
    private final CommonSettings.Common common;
    private final CredentialVerifier verifier;
    private final @Nullable RequestHandler handler;
    private final @Nullable ConnectionHandlerFactory factory;
    private final @Nullable Consumer<PreparationFailure> failureListener;
    private final UnreleasedHandlers unreleased = new UnreleasedHandlers();
    private final OwnerShutdown lifecycle;

    private NioStrandServer(
            CommonSettings.Common common,
            CredentialVerifier verifier,
            @Nullable RequestHandler handler,
            @Nullable ConnectionHandlerFactory factory,
            @Nullable Consumer<PreparationFailure> failureListener) {
        this.common = common;
        this.verifier = verifier;
        this.handler = handler;
        this.factory = factory;
        this.failureListener = failureListener;
        this.lifecycle = new OwnerShutdown(common, unreleased);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends CommonSettings<Builder> {
        private @Nullable CredentialVerifier verifier;
        private @Nullable RequestHandler handler;
        private @Nullable ConnectionHandlerFactory factory;
        private @Nullable Consumer<PreparationFailure> failureListener;

        private Builder() {}

        @Override
        Builder self() {
            return this;
        }

        @Override
        void collectMissing(List<String> missing) {
            if (verifier == null) {
                missing.add("authenticate");
            }
            if (handler == null && factory == null) {
                missing.add("handler 또는 handlerFactory");
            }
        }

        public Builder authenticate(CredentialVerifier value) {
            this.verifier = Objects.requireNonNull(value, "authenticate");
            return this;
        }

        public Builder handler(RequestHandler value) {
            this.handler = Objects.requireNonNull(value, "handler");
            return this;
        }

        public Builder handlerFactory(ConnectionHandlerFactory value) {
            this.factory = Objects.requireNonNull(value, "handlerFactory");
            return this;
        }

        public Builder onConnectionFailure(Consumer<PreparationFailure> value) {
            this.failureListener = Objects.requireNonNull(value, "onConnectionFailure");
            return this;
        }

        public NioStrandServer build() {
            if (handler != null && factory != null) {
                throw new IllegalArgumentException("handler와 handlerFactory는 함께 쓸 수 없다");
            }
            Common common = validate();
            return new NioStrandServer(common, Objects.requireNonNull(verifier), handler, factory, failureListener);
        }
    }

    public CompletionStage<Listener> listen(SocketAddress address) {
        Objects.requireNonNull(address, "address");
        CompletableFuture<Listener> future = new CompletableFuture<>();
        if (!lifecycle.accepting()) {
            future.completeExceptionally(new IllegalStateException("전체 종료를 시작해 새 리스닝을 받지 않는다"));
            return future.thenApply(listener -> listener);
        }
        try {
            ServerSocketChannel server = ServerSocketChannel.open();
            try {
                server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                server.bind(address);
                Listener listener = new Listener(
                        server,
                        server.getLocalAddress(),
                        this::admit,
                        common.executor,
                        common.shutdownDeadline.toNanos());
                if (lifecycle.register(listener)) {
                    future.complete(listener);
                } else {

                    listener.close();
                    future.completeExceptionally(new IllegalStateException("전체 종료를 시작해 새 리스닝을 받지 않는다"));
                }
            } catch (IOException | RuntimeException e) {
                closeQuietly(server);
                throw e;
            }
        } catch (IOException | RuntimeException e) {
            future.completeExceptionally(e);
        }
        return future.thenApply(listener -> listener);
    }

    private static void closeQuietly(ServerSocketChannel server) {
        try {
            server.close();
        } catch (IOException ignored) {

        }
    }

    private void admit(SocketChannel channel) {
        long now = common.nanoClock.getAsLong();
        SocketAddress remote;
        try {

            remote = Objects.requireNonNull(channel.getRemoteAddress());
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        } catch (IOException e) {
            PassageFactory.closeQuietly(channel);
            return;
        }
        ConnectionBudgets budgets = common.owner.newConnection(common.limits);
        Resource denied = ConnectionEngine.admit(budgets, common.space);
        if (denied != null) {
            PassageFactory.closeQuietly(channel);
            report(new PreparationFailure(remote, PreparationStage.CONNECTION, new ResourceExhaustedException(denied)));
            return;
        }
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.SERVER,
                common.endpointConfig(),
                remaining -> PassageFactory.open(channel, common.security.tlsContext(), null, 0),
                remote,
                new SerialExecutor(common.executor),
                common.executor,
                budgets,
                common.space,
                common.maxFrameSize,
                common.deadlineAt(now),
                common.closeFinishNanos,
                common.writeQueueBytes,
                common.nanoClock,
                new ConnectionParams.PreparationObserver() {
                    @Override
                    public void ready(NioStrandConnection connection) {}

                    @Override
                    public void failed(PreparationFailure failure) {
                        deliver(failure);
                    }

                    @Override
                    public void rejected(PreparationFailure failure) {}
                },
                handler,
                factory,
                verifier,
                common.security.tlsContext() != null);
        params.unreleased = unreleased;
        params.passageWrapper = common.passageWrapper;
        NioStrandConnection connection = new NioStrandConnection(params);
        if (!lifecycle.register(connection)) {

            ConnectionEngine.cancelAdmission(budgets, common.space);
            PassageFactory.closeQuietly(channel);
            return;
        }
        params.terminated = () -> lifecycle.unregister(connection);
        try {
            connection.start();
        } catch (RuntimeException | Error e) {
            lifecycle.unregister(connection);
            ConnectionEngine.cancelAdmission(budgets, common.space);
            throw e;
        }
    }

    private void report(PreparationFailure failure) {
        try {
            common.executor.execute(() -> deliver(failure));
        } catch (RuntimeException ignored) {

        }
    }

    private void deliver(PreparationFailure failure) {
        Consumer<PreparationFailure> listener = failureListener;
        if (listener == null) {
            return;
        }
        try {
            listener.accept(failure);
        } catch (RuntimeException ignored) {

        }
    }

    public List<UnreleasedHandler> unreleasedHandlers() {
        return unreleased.snapshot();
    }

    public ResourceUsage resourceUsage(Resource resource) {
        return common.owner.usage(resource);
    }

    public CompletionStage<Void> shutdown() {
        return lifecycle.shutdown();
    }

    public Result<ShutdownResult> shutdownResult() {
        return lifecycle.result();
    }

    public CompletionStage<Void> resourcesTerminated() {
        return lifecycle.terminated();
    }
}
