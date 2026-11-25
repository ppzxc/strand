package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ShutdownResult;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.SerialExecutor;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import io.github.ppzxc.strand.netty.internal.StrandServerChannel;
import io.github.ppzxc.strand.netty.internal.WireFactory;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

public final class NettyStrandServer {
    private final CommonSettings.Common common;
    private final CredentialVerifier verifier;
    private final @Nullable RequestHandler handler;
    private final @Nullable ConnectionHandlerFactory factory;
    private final @Nullable Consumer<PreparationFailure> failureListener;
    private final UnreleasedHandlers unreleased = new UnreleasedHandlers();
    private final OwnerShutdown lifecycle;

    private NettyStrandServer(
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

        public NettyStrandServer build() {
            if (handler != null && factory != null) {
                throw new IllegalArgumentException("handler와 handlerFactory는 함께 쓸 수 없다");
            }
            Common common = validate();
            return new NettyStrandServer(common, Objects.requireNonNull(verifier), handler, factory, failureListener);
        }
    }

    public Future<Listener> listen(SocketAddress address) {
        return listen(address, Transport.tcp());
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    public Future<Listener> listen(SocketAddress address, Transport transport) {
        Objects.requireNonNull(address, "address");
        Objects.requireNonNull(transport, "transport");
        if (transport.isWebSocket()) {
            WireFactory.requireCodec();
        }
        EventLoop loop = common.group.next();
        Promise<Listener> promise = loop.newPromise();
        StrandServerChannel server = new StrandServerChannel();
        Listener listener = new Listener(server, loop);
        if (!lifecycle.register(listener)) {
            listener.close();
            promise.tryFailure(new IllegalStateException("전체 종료를 시작해 새 리스닝을 받지 않는다"));
            return promise;
        }
        try {
            server.config().setOption(ChannelOption.SO_REUSEADDR, true);
            server.pipeline().addLast(new Acceptor(transport));
            loop.register(server).addListener((ChannelFutureListener) registered -> {
                if (!registered.isSuccess()) {
                    listener.close();
                    promise.tryFailure(registered.cause());
                    return;
                }
                server.bind(address).addListener((ChannelFutureListener) bound -> {
                    if (bound.isSuccess()) {
                        listener.bound();
                        promise.trySuccess(listener);
                    } else {
                        server.close();
                        promise.tryFailure(bound.cause());
                    }
                });
            });
        } catch (RuntimeException e) {
            listener.close();
            promise.tryFailure(e);
        }
        return promise;
    }

    final class Acceptor extends ChannelInboundHandlerAdapter {
        private final Transport transport;

        Acceptor() {
            this(Transport.tcp());
        }

        Acceptor(Transport transport) {
            this.transport = transport;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            StrandChannel accepted = (StrandChannel) message;
            if (!ctx.channel().isOpen()) {
                accepted.unsafe().closeForcibly();
                return;
            }
            try {
                admit(accepted, transport);
            } catch (RuntimeException e) {
                accepted.unsafe().closeForcibly();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {}
    }

    private void admit(StrandChannel channel, Transport transport) {
        long now = common.nanoClock.getAsLong();
        SocketAddress remote = Objects.requireNonNull(channel.remoteAddress());
        ConnectionBudgets budgets = common.owner.newConnection(common.limits);
        Resource denied = ConnectionEngine.admit(budgets, common.space);
        if (denied != null) {
            channel.unsafe().closeForcibly();
            report(new PreparationFailure(remote, PreparationStage.CONNECTION, new ResourceExhaustedException(denied)));
            return;
        }
        EventLoop loop = common.group.next();
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.SERVER,
                common.endpointConfig(),
                remote,
                loop,
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
                    public void ready(NettyStrandConnection connection) {}

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
                common.tls(null, 0),
                common.allocator);
        params.unreleased = unreleased;
        params.transport = transport;
        NettyStrandConnection connection = new NettyStrandConnection(params, channel);
        params.terminated = () -> lifecycle.unregister(connection);
        if (!lifecycle.register(connection)) {
            ConnectionEngine.cancelAdmission(budgets, common.space);
            channel.unsafe().closeForcibly();
            return;
        }
        try {
            channel.config().setTcpNoDelay(true);
            connection.startServer();
        } catch (RuntimeException | Error e) {
            lifecycle.unregister(connection);
            ConnectionEngine.cancelAdmission(budgets, common.space);
            throw e;
        }
    }

    @SuppressWarnings("EmptyCatch")
    private void report(PreparationFailure failure) {
        try {
            common.executor.execute(() -> deliver(failure));
        } catch (RuntimeException ignored) {
        }
    }

    @SuppressWarnings("EmptyCatch")
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

    public Future<Void> shutdown() {
        return lifecycle.shutdown();
    }

    public Result<ShutdownResult> shutdownResult() {
        return lifecycle.result();
    }

    public Future<Void> resourcesTerminated() {
        return lifecycle.terminated();
    }
}
