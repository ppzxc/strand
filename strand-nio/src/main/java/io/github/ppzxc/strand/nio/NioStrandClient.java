package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.ShutdownResult;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.jspecify.annotations.Nullable;

public final class NioStrandClient {
    private final CommonSettings.Common common;
    private final EndpointConfigSource configSource;
    private final @Nullable RequestHandler handler;
    private final TcpConnector.Resolver resolver;
    private final long credentialBytes;
    private final OwnerShutdown lifecycle;

    private NioStrandClient(
            CommonSettings.Common common,
            EndpointConfigSource configSource,
            @Nullable RequestHandler handler,
            TcpConnector.Resolver resolver,
            long credentialBytes) {
        this.resolver = resolver;
        this.credentialBytes = credentialBytes;
        this.common = common;
        this.configSource = configSource;
        this.handler = handler;
        this.lifecycle = new OwnerShutdown(common, null);
    }

    private interface EndpointConfigSource {
        io.github.ppzxc.strand.core.internal.EndpointConfig create();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends CommonSettings<Builder> {
        private @Nullable Credentials credentials;
        private @Nullable RequestHandler handler;
        private TcpConnector.Resolver resolver = java.net.InetAddress::getAllByName;

        private Builder() {}

        Builder resolver(TcpConnector.Resolver value) {
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

        public NioStrandClient build() {
            Common common = validate();
            Credentials auth = credentials;
            return new NioStrandClient(
                    common,
                    () -> {
                        io.github.ppzxc.strand.core.internal.EndpointConfig config = common.endpointConfig();
                        return auth == null ? config : config.auth(auth.mechanism(), auth.secret());
                    },
                    handler,
                    resolver,
                    auth == null ? 0 : auth.mechanism().length() + auth.secret().length);
        }
    }

    public CompletionStage<NioStrandConnection> connect(SocketAddress address) {
        return connectAttempt(address).result();
    }

    public NioConnectAttempt connectAttempt(SocketAddress address) {
        if (!(Objects.requireNonNull(address, "address") instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("TCP 주소(InetSocketAddress)여야 한다");
        }
        InetSocketAddress target = (InetSocketAddress) address;
        CompletableFuture<NioStrandConnection> future = new CompletableFuture<>();
        CompletableFuture<Void> released = new CompletableFuture<>();
        ConnectAttemptState state = new ConnectAttemptState();

        NioConnectAttempt attempt = new NioConnectAttempt(
                future.thenApply(connection -> connection), released.thenApply(done -> null), state);
        if (!lifecycle.accepting()) {
            failWith(future, released, target, new IllegalStateException("전체 종료를 시작해 새 연결 시도를 받지 않는다"));
            return attempt;
        }
        long now = common.nanoClock.getAsLong();
        io.github.ppzxc.strand.nio.internal.ConnectionBudgets budgets = common.owner.newConnection(common.limits);
        Resource denied = ConnectionEngine.admit(budgets, common.space);
        if (denied != null) {
            fail(future, released, target, denied);
            return attempt;
        }

        PreparationBytes preparationBytes = new PreparationBytes(budgets);
        if (!preparationBytes.tryReserve(retainedInputBytes(target))) {
            ConnectionEngine.cancelAdmission(budgets, common.space);
            fail(future, released, target, Resource.PREPARATION_BYTES);
            return attempt;
        }
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.CLIENT,
                configSource.create(),
                new TcpConnector(
                        budgets,
                        target,
                        common.security.tlsContext(),
                        resolver,
                        common.executor,
                        state,
                        preparationBytes,
                        common.nanoClock),
                target,
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
                    public void ready(NioStrandConnection connection) {
                        future.complete(connection);
                    }

                    @Override
                    public void failed(PreparationFailure failure) {
                        future.completeExceptionally(new PreparationFailedException(failure));
                    }
                },
                handler,
                null,
                null,
                common.security.tlsContext() != null);
        params.attempt = state;
        params.passageWrapper = common.passageWrapper;
        params.preparationBytes = preparationBytes;
        params.released = released;
        params.releasedFallbackNanos = common.shutdownDeadline.toNanos();
        NioStrandConnection connection = new NioStrandConnection(params);
        if (!lifecycle.register(connection)) {

            preparationBytes.releaseAll();
            ConnectionEngine.cancelAdmission(budgets, common.space);
            failWith(future, released, target, new IllegalStateException("전체 종료를 시작해 새 연결 시도를 받지 않는다"));
            return attempt;
        }
        params.terminated = () -> lifecycle.unregister(connection);
        try {
            connection.start();
        } catch (RuntimeException | Error e) {
            lifecycle.unregister(connection);
            preparationBytes.releaseAll();
            ConnectionEngine.cancelAdmission(budgets, common.space);
            future.completeExceptionally(e);
            released.complete(null);
        }
        return attempt;
    }

    private static void fail(
            CompletableFuture<NioStrandConnection> future,
            CompletableFuture<Void> released,
            InetSocketAddress target,
            Resource denied) {
        failWith(future, released, target, new ResourceExhaustedException(denied));
    }

    private static void failWith(
            CompletableFuture<NioStrandConnection> future,
            CompletableFuture<Void> released,
            InetSocketAddress target,
            RuntimeException cause) {
        future.completeExceptionally(
                new PreparationFailedException(new PreparationFailure(target, PreparationStage.CONNECTION, cause)));
        released.complete(null);
    }

    private long retainedInputBytes(InetSocketAddress target) {
        long bytes = target.getHostString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        return bytes + credentialBytes;
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
