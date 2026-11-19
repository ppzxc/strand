package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.SerialExecutor;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Netty 클라이언트. {@link #connect}는 보안·HELLO·연결 인증·핸들러 등록을 마치면 성공하는 Netty {@link Future}를 돌려준다. 호출 스레드는 막히지 않는다.
 * 이 객체는 앱이 준 {@code EventLoopGroup}과 실행기를 종료하지 않는다.
 */
public final class NettyStrandClient {
    private final CommonSettings.Common common;
    private final EndpointConfigSource configSource;
    private final @Nullable RequestHandler handler;
    private final HostResolver resolver;

    private NettyStrandClient(
            CommonSettings.Common common,
            EndpointConfigSource configSource,
            @Nullable RequestHandler handler,
            HostResolver resolver) {
        this.common = common;
        this.configSource = configSource;
        this.handler = handler;
        this.resolver = resolver;
    }

    /** 연결마다 쓸 core 설정(자격 증명 포함)을 만든다. */
    private interface EndpointConfigSource {
        EndpointConfig create();
    }

    /**
     * builder를 만든다.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** {@link NettyStrandClient} builder. */
    public static final class Builder extends CommonSettings<Builder> {
        private @Nullable Credentials credentials;
        private @Nullable RequestHandler handler;
        private HostResolver resolver = java.net.InetAddress::getAllByName;

        private Builder() {}

        /** 테스트가 주소 해석을 제어하려고 바꾼다. */
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

        /**
         * 연결 인증(AUTH)에 보낼 자격 증명. 정하지 않으면 {@code ANONYMOUS}와 빈 자격 증명을 보낸다.
         *
         * @param value 자격 증명
         * @return 이 builder
         */
        public Builder credentials(Credentials value) {
            this.credentials = Objects.requireNonNull(value, "credentials");
            return this;
        }

        /**
         * 서버가 낸 요청을 받는 핸들러. 정하지 않으면 받은 요청에 {@code UNIMPLEMENTED}로 답한다. 연결 시작 전에 등록하며 연결 성공 때 이미 등록돼 있다.
         *
         * @param value 핸들러
         * @return 이 builder
         */
        public Builder handler(RequestHandler value) {
            this.handler = Objects.requireNonNull(value, "handler");
            return this;
        }

        /**
         * 설정을 검증하고 클라이언트를 만든다.
         *
         * @return 클라이언트
         * @throws IllegalStateException 필수 설정이 없을 때
         * @throws IllegalArgumentException 범위 오류, 최소 진행 공간 부족일 때
         */
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
                    resolver);
        }
    }

    /**
     * 서버에 연결한다. {@link #connectAttempt}의 {@link NettyConnectAttempt#result()}와 같다. 성공은 TCP 연결·채널 보안·HELLO·연결 인증·핸들러
     * 등록이 모두 끝났다는 뜻이다. 실패는 {@link PreparationFailedException}이고 단계와 원인을 알린다. 반환된 Future는 취소할 수 없고 연결 시도를 취소하는 길은
     * {@link #connectAttempt}의 {@link NettyConnectAttempt#cancel()}뿐이다. Future는 연결 채널의 EventLoop에서 완료된다.
     *
     * @param address 서버 주소({@link InetSocketAddress})
     * @return 연결 결과
     * @throws IllegalArgumentException 주소가 {@link InetSocketAddress}가 아닐 때
     */
    public Future<NettyStrandConnection> connect(SocketAddress address) {
        return connectAttempt(address).result();
    }

    /**
     * 취소할 수 있는 연결 시도를 시작한다. {@link #connect}와 같은 준비를 하며 {@link NettyConnectAttempt}로 결과 관찰·취소·채널 정리 관찰을 따로 한다. 호출
     * 스레드는 막히지 않는다. 주소 해석이 필요하면 앱용 실행기에서 하고, 해석된 주소는 하나씩 순서대로 연결하며 모든 시도가 처음 정한 준비 기한을 공유한다.
     *
     * @param address 서버 주소({@link InetSocketAddress})
     * @return 연결 시도
     * @throws IllegalArgumentException 주소가 {@link InetSocketAddress}가 아닐 때
     */
    public NettyConnectAttempt connectAttempt(SocketAddress address) {
        if (!(Objects.requireNonNull(address, "address") instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("TCP 주소(InetSocketAddress)여야 한다");
        }
        InetSocketAddress target = (InetSocketAddress) address;
        EventLoop loop = common.group.next();
        // 앱이 받는 Future는 취소할 수 없게 한다. 취소를 받아들이면 성공한 연결이 주인 없이 남는다.
        Promise<NettyStrandConnection> promise = loop.newPromise();
        promise.setUncancellable();
        Promise<Void> released = loop.newPromise();
        released.setUncancellable();
        ConnectAttemptState state = new ConnectAttemptState();
        NettyConnectAttempt attempt = new NettyConnectAttempt(promise, released, state);
        long now = common.nanoClock.getAsLong();
        ConnectionBudgets budgets = common.owner.newConnection(common.limits);
        Resource denied = ConnectionEngine.admit(budgets, common.space);
        if (denied != null) {
            promise.tryFailure(new PreparationFailedException(new PreparationFailure(
                    target, PreparationStage.CONNECTION, new ResourceExhaustedException(denied))));
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
        params.resolver = resolver;
        try {
            StrandChannel channel = new StrandChannel();
            channel.config().setTcpNoDelay(true);
            new NettyStrandConnection(params, channel).startClient(target);
        } catch (RuntimeException | Error e) {
            ConnectionEngine.cancelAdmission(budgets, common.space);
            promise.tryFailure(e);
            released.trySuccess(null);
        }
        return attempt;
    }

    /**
     * 소유 객체 전체 범위의 한도 사용량 집계(설정값·현재량·최고량·거절 횟수). 자격 증명은 담지 않는다.
     *
     * @param resource 조회할 종류
     * @return 사용량
     * @throws IllegalArgumentException 소유 객체 범위에 없는 종류일 때
     */
    public ResourceUsage resourceUsage(Resource resource) {
        return common.owner.usage(resource);
    }
}
