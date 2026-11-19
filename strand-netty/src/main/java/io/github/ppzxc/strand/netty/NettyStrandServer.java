package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.SerialExecutor;
import io.github.ppzxc.strand.netty.internal.StrandChannel;
import io.github.ppzxc.strand.netty.internal.StrandServerChannel;
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

/**
 * NIO 서버. {@link #listen}은 바인딩이 끝나면 성공한다. 연결마다 TCP 수락부터 준비 기한을 세고, 개별 연결의 준비 실패는 리스닝 성공을 바꾸거나 중단하지
 * 않고 {@code onConnectionFailure}로 관찰한다. 이 객체는 앱이 준 EventLoopGroup과 실행기를 종료하지 않는다.
 */
public final class NettyStrandServer {
    private final CommonSettings.Common common;
    private final CredentialVerifier verifier;
    private final @Nullable RequestHandler handler;
    private final @Nullable ConnectionHandlerFactory factory;
    private final @Nullable Consumer<PreparationFailure> failureListener;
    private final UnreleasedHandlers unreleased = new UnreleasedHandlers();

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
    }

    /**
     * builder를 만든다.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** {@link NettyStrandServer} builder. */
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

        /**
         * 연결 인증(AUTH)을 판단하는 함수. 앱용 실행기에서 불린다. 필수다.
         *
         * @param value 판단 함수
         * @return 이 builder
         */
        public Builder authenticate(CredentialVerifier value) {
            this.verifier = Objects.requireNonNull(value, "authenticate");
            return this;
        }

        /**
         * 모든 연결이 공유하는 핸들러. 인증 직후 준비가 끝난다. {@link #handlerFactory}와 함께 쓸 수 없다.
         *
         * @param value 핸들러
         * @return 이 builder
         */
        public Builder handler(RequestHandler value) {
            this.handler = Objects.requireNonNull(value, "handler");
            return this;
        }

        /**
         * 인증된 연결마다 핸들러를 만드는 팩터리. 팩터리가 끝나기 전에 온 요청은 {@code REFUSED}로 거절한다. {@link #handler}와 함께 쓸 수 없다.
         *
         * @param value 팩터리
         * @return 이 builder
         */
        public Builder handlerFactory(ConnectionHandlerFactory value) {
            this.factory = Objects.requireNonNull(value, "handlerFactory");
            return this;
        }

        /**
         * 개별 연결의 준비 실패(상대 주소·단계·원인)를 관찰한다. 앱용 실행기에서 불린다. 자격 증명 원문은 담기지 않는다.
         *
         * @param value 실패 관찰자
         * @return 이 builder
         */
        public Builder onConnectionFailure(Consumer<PreparationFailure> value) {
            this.failureListener = Objects.requireNonNull(value, "onConnectionFailure");
            return this;
        }

        /**
         * 설정을 검증하고 서버를 만든다.
         *
         * @return 서버
         * @throws IllegalStateException 필수 설정이 없을 때
         * @throws IllegalArgumentException 범위 오류, 최소 진행 공간 부족, handler와 handlerFactory를 함께 정했을 때
         */
        public NettyStrandServer build() {
            if (handler != null && factory != null) {
                throw new IllegalArgumentException("handler와 handlerFactory는 함께 쓸 수 없다");
            }
            Common common = validate();
            return new NettyStrandServer(common, Objects.requireNonNull(verifier), handler, factory, failureListener);
        }
    }

    /**
     * 주소에 바인딩해 연결 수락을 시작한다. 성공은 바인딩 완료다. 바인딩 실패는 Future의 비동기 실패다. Future는 리스닝 채널의 EventLoop에서 완료된다.
     *
     * @param address 바인딩할 주소
     * @return 리스닝 결과
     */
    @SuppressWarnings("FutureReturnValueIgnored") // 바인딩 실패 뒤 닫기는 이미 실패한 결과 위에서 하므로 관찰할 것이 없다.
    public Future<Listener> listen(SocketAddress address) {
        Objects.requireNonNull(address, "address");
        EventLoop loop = common.group.next();
        Promise<Listener> promise = loop.newPromise();
        StrandServerChannel server = new StrandServerChannel();
        try {
            server.config().setOption(ChannelOption.SO_REUSEADDR, true);
            server.pipeline().addLast(new Acceptor());
            loop.register(server).addListener((ChannelFutureListener) registered -> {
                if (!registered.isSuccess()) {
                    promise.tryFailure(registered.cause());
                    return;
                }
                server.bind(address).addListener((ChannelFutureListener) bound -> {
                    if (bound.isSuccess()) {
                        promise.trySuccess(new Listener(server));
                    } else {
                        server.close();
                        promise.tryFailure(bound.cause());
                    }
                });
            });
        } catch (RuntimeException e) {
            server.unsafe().closeForcibly();
            promise.tryFailure(e);
        }
        return promise;
    }

    /** 리스닝 채널이 수락한 채널을 받아 연결로 등록한다. 리스닝 채널이 이미 닫혔다면 경합으로 수락된 채널을 정리한다. */
    private final class Acceptor extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            StrandChannel accepted = (StrandChannel) message;
            if (!ctx.channel().isOpen()) {
                accepted.unsafe().closeForcibly();
                return;
            }
            try {
                admit(accepted);
            } catch (RuntimeException e) {
                // 한 연결의 등록 실패가 리스닝을 끝내지 않게 한다. 등록하지 못한 채널은 정리한다.
                accepted.unsafe().closeForcibly();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // 수락 중의 일시적 오류(예: 파일 기술자 부족)는 리스닝을 유지한다.
        }
    }

    /** 수락한 채널을 연결로 등록한다. 슬롯과 필수 진행 공간을 수락 직후 예약하고 모자라면 채널을 즉시 닫는다. */
    private void admit(StrandChannel channel) {
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
                    public void rejected(PreparationFailure failure) {
                        // 앱용 실행기가 알림을 거절했다. EventLoop에서 사용자 코드(관찰자)를 대신 실행하지 않고 관찰할 수 없는 실패는 버린다.
                    }
                },
                handler,
                factory,
                verifier,
                common.tls(null, 0),
                common.allocator);
        params.unreleased = unreleased;
        try {
            channel.config().setTcpNoDelay(true);
            new NettyStrandConnection(params, channel).startServer();
        } catch (RuntimeException | Error e) {
            ConnectionEngine.cancelAdmission(budgets, common.space);
            throw e;
        }
    }

    /** 연결의 직렬 실행기가 아닌 곳(접수 거절)에서 앱용 실행기로 실패를 알린다. 거절되면 버린다. */
    private void report(PreparationFailure failure) {
        try {
            common.executor.execute(() -> deliver(failure));
        } catch (RuntimeException ignored) {
            // 실행기가 거절했다. 관찰할 수 없는 실패는 버린다.
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
            // 관찰자의 실패는 연결 준비 결과를 바꾸지 않는다.
        }
    }

    /**
     * 폐기 실행이 거절돼 앱이 회수할 수 있는 팩터리 결과의 조회. 조회는 소유권을 옮기지 않는다.
     *
     * @return 회수할 수 있는 결과
     */
    public List<UnreleasedHandler> unreleasedHandlers() {
        return unreleased.snapshot();
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
