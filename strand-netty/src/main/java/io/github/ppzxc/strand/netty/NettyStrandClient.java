package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ShutdownResult;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.SerialExecutor;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import io.github.ppzxc.strand.netty.internal.WireFactory;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public final class NettyStrandClient {
    private final CommonSettings.Common common;
    private final EndpointConfigSource configSource;
    private final @Nullable RequestHandler handler;
    private final HostResolver resolver;
    private final long credentialBytes;
    private final OwnerShutdown lifecycle;

    private NettyStrandClient(
            CommonSettings.Common common,
            EndpointConfigSource configSource,
            @Nullable RequestHandler handler,
            HostResolver resolver,
            long credentialBytes) {
        this.credentialBytes = credentialBytes;
        this.common = common;
        this.configSource = configSource;
        this.handler = handler;
        this.resolver = resolver;
        this.lifecycle = new OwnerShutdown(common, null);
    }

    private interface EndpointConfigSource {
        EndpointConfig create();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends CommonSettings<Builder> {
        private @Nullable Credentials credentials;
        private @Nullable RequestHandler handler;
        private HostResolver resolver = HostResolver.SYSTEM;

        private Builder() {}

        Builder resolver(HostResolver value) {
            this.resolver = Objects.requireNonNull(value, "resolver");
            return this;
        }

        @Override
        Builder self() {
            return this;
        }

        @Override
        void collectMissing(List<String> missing) {}

        public Builder credentials(Credentials value) {
            this.credentials = Objects.requireNonNull(value, "credentials");
            return this;
        }

        public Builder handler(RequestHandler value) {
            this.handler = Objects.requireNonNull(value, "handler");
            return this;
        }

        public NettyStrandClient build() {
            Common common = validate();
            Credentials auth = credentials;
            return new NettyStrandClient(
                    common,
                    () -> {
                        EndpointConfig config = common.endpointConfig();
                        return auth == null ? config : config.auth(auth.mechanism(), auth.secret());
                    },
                    handler,
                    resolver,
                    auth == null ? 0 : auth.mechanism().length() + auth.secret().length);
        }
    }

    public Future<NettyStrandConnection> connect(SocketAddress address) {
        return connect(address, Transport.tcp());
    }

    public Future<NettyStrandConnection> connect(SocketAddress address, Transport transport) {
        return connectAttempt(address, transport).result();
    }

    public NettyConnectAttempt connectAttempt(SocketAddress address) {
        return connectAttempt(address, Transport.tcp());
    }

    public NettyConnectAttempt connectAttempt(SocketAddress address, Transport transport) {
        Objects.requireNonNull(transport, "transport");
        if (transport.isWebSocket()) {
            WireFactory.requireCodec();
        }
        if (!(Objects.requireNonNull(address, "address") instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("TCP 주소(InetSocketAddress)여야 한다");
        }
        InetSocketAddress target = (InetSocketAddress) address;
        EventLoop loop = common.group.next();
        Promise<NettyStrandConnection> promise = loop.newPromise();
        promise.setUncancellable();
        Promise<Void> released = loop.newPromise();
        released.setUncancellable();
        ConnectAttemptState state = new ConnectAttemptState();
        NettyConnectAttempt attempt = new NettyConnectAttempt(promise, released, state);
        if (!lifecycle.accepting()) {
            failShutdown(promise, released, target);
            return attempt;
        }
        long now = common.nanoClock.getAsLong();
        ConnectionBudgets budgets = common.owner.newConnection(common.limits);
        Resource denied = ConnectionEngine.admit(budgets, common.space);
        if (denied != null) {
            promise.tryFailure(new PreparationFailedException(new PreparationFailure(
                    target, PreparationStage.CONNECTION, new ResourceExhaustedException(denied))));
            released.trySuccess(null);
            return attempt;
        }
        PreparationBytes preparationBytes = new PreparationBytes(budgets);
        if (!preparationBytes.tryReserve(retainedInputBytes(target))) {
            ConnectionEngine.cancelAdmission(budgets, common.space);
            promise.tryFailure(new PreparationFailedException(new PreparationFailure(
                    target, PreparationStage.CONNECTION, new ResourceExhaustedException(Resource.PREPARATION_BYTES))));
            released.trySuccess(null);
            return attempt;
        }
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.CLIENT,
                configSource.create(),
                target,
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
                    public void ready(NettyStrandConnection connection) {
                        promise.trySuccess(connection);
                    }

                    @Override
                    public void failed(PreparationFailure failure) {
                        promise.tryFailure(new PreparationFailedException(failure));
                    }
                },
                handler,
                null,
                null,
                common.tls(target.getHostString(), target.getPort()),
                common.allocator);
        params.attempt = state;
        params.released = released;
        params.preparationBytes = preparationBytes;
        params.resolver = resolver;
        params.transport = transport;
        StrandChannel channel = new StrandChannel();
        NettyStrandConnection connection = new NettyStrandConnection(params, channel);
        params.terminated = () -> lifecycle.unregister(connection);
        if (!lifecycle.register(connection)) {
            preparationBytes.releaseAll();
            ConnectionEngine.cancelAdmission(budgets, common.space);
            failShutdown(promise, released, target);
            return attempt;
        }
        try {
            channel.config().setTcpNoDelay(true);
            connection.startClient(target);
        } catch (RuntimeException | Error e) {
            lifecycle.unregister(connection);
            preparationBytes.releaseAll();
            ConnectionEngine.cancelAdmission(budgets, common.space);
            promise.tryFailure(e);
            released.trySuccess(null);
        }
        return attempt;
    }

    private static void failShutdown(
            Promise<NettyStrandConnection> promise, Promise<Void> released, InetSocketAddress target) {
        promise.tryFailure(new PreparationFailedException(new PreparationFailure(
                target, PreparationStage.CONNECTION, new IllegalStateException("전체 종료를 시작해 새 연결 시도를 받지 않는다"))));
        released.trySuccess(null);
    }

    private long retainedInputBytes(InetSocketAddress target) {
        long bytes = target.getHostString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        return bytes + credentialBytes;
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
