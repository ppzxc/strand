package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.api.ResourceUsage;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.jspecify.annotations.Nullable;

/**
 * NIO 클라이언트. {@link #connect}는 보안·HELLO·연결 인증·핸들러 등록을 마치면 성공하는 {@link CompletionStage}를 돌려준다. 호출 스레드는 막히지 않는다.
 * 이 객체는 앱이 준 실행기를 종료하지 않는다.
 */
public final class NioStrandClient {
    private final CommonSettings.Common common;
    private final EndpointConfigSource configSource;
    private final @Nullable RequestHandler handler;

    private NioStrandClient(
            CommonSettings.Common common, EndpointConfigSource configSource, @Nullable RequestHandler handler) {
        this.common = common;
        this.configSource = configSource;
        this.handler = handler;
    }

    /** 연결마다 쓸 core 설정(자격 증명 포함)을 만든다. */
    private interface EndpointConfigSource {
        io.github.ppzxc.strand.core.internal.EndpointConfig create();
    }

    /**
     * builder를 만든다.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** {@link NioStrandClient} builder. */
    public static final class Builder extends CommonSettings<Builder> {
        private @Nullable Credentials credentials;
        private @Nullable RequestHandler handler;

        private Builder() {}

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
        public NioStrandClient build() {
            Common common = validate();
            Credentials auth = credentials;
            return new NioStrandClient(
                    common,
                    () -> {
                        io.github.ppzxc.strand.core.internal.EndpointConfig config = common.endpointConfig();
                        return auth == null ? config : config.auth(auth.mechanism(), auth.secret());
                    },
                    handler);
        }
    }

    /**
     * 서버에 연결한다. 성공은 TCP 연결·채널 보안·HELLO·연결 인증·핸들러 등록이 모두 끝났다는 뜻이다. 실패는 {@link PreparationFailedException}이고 단계와
     * 원인을 알린다. 반환된 Stage를 취소해도 연결 시도는 취소되지 않는다.
     *
     * @param address 서버 주소({@link InetSocketAddress})
     * @return 연결 결과
     * @throws IllegalArgumentException 주소가 {@link InetSocketAddress}가 아닐 때
     */
    public CompletionStage<NioStrandConnection> connect(SocketAddress address) {
        if (!(Objects.requireNonNull(address, "address") instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("TCP 주소(InetSocketAddress)여야 한다");
        }
        InetSocketAddress target = (InetSocketAddress) address;
        CompletableFuture<NioStrandConnection> future = new CompletableFuture<>();
        long now = common.nanoClock.getAsLong();
        io.github.ppzxc.strand.nio.internal.ConnectionBudgets budgets = common.owner.newConnection(common.limits);
        Resource denied = ConnectionEngine.admit(budgets, common.space);
        if (denied != null) {
            future.completeExceptionally(new PreparationFailedException(new PreparationFailure(
                    target, PreparationStage.CONNECTION, new ResourceExhaustedException(denied))));
            return future.thenApply(connection -> connection);
        }
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.CLIENT,
                configSource.create(),
                remaining -> openChannel(budgets, target, remaining),
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
        try {
            new NioStrandConnection(params).start();
        } catch (RuntimeException e) {
            ConnectionEngine.cancelAdmission(budgets, common.space);
            future.completeExceptionally(e);
        }
        return future.thenApply(connection -> connection);
    }

    private io.github.ppzxc.strand.nio.internal.Passage openChannel(
            io.github.ppzxc.strand.nio.internal.ConnectionBudgets budgets,
            InetSocketAddress address,
            long remainingNanos)
            throws IOException {
        InetSocketAddress target = resolve(budgets, address);
        long millis = remainingNanos / 1_000_000L + 1;
        if (remainingNanos <= 0) {
            throw new SocketTimeoutException("준비 기한 안에 연결하지 못했다");
        }
        SocketChannel channel = SocketChannel.open();
        try {
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.socket().connect(target, (int) Math.min(millis, Integer.MAX_VALUE));
        } catch (IOException | RuntimeException e) {
            PassageFactory.closeQuietly(channel);
            throw e;
        }
        return PassageFactory.open(channel, common.security.tlsContext(), target.getHostString(), target.getPort());
    }

    /** 해석하지 않은 주소는 주소 해석 작업 슬롯을 예약해 해석한다. */
    private static InetSocketAddress resolve(
            io.github.ppzxc.strand.nio.internal.ConnectionBudgets budgets, InetSocketAddress address)
            throws IOException {
        if (!address.isUnresolved()) {
            return address;
        }
        if (!budgets.owner().tryReserve(Resource.RESOLVE_TASKS, 1)) {
            throw new IOException(new ResourceExhaustedException(Resource.RESOLVE_TASKS));
        }
        try {
            InetSocketAddress resolved = new InetSocketAddress(address.getHostString(), address.getPort());
            if (resolved.isUnresolved()) {
                throw new UnknownHostException(address.getHostString());
            }
            return resolved;
        } finally {
            budgets.owner().release(Resource.RESOLVE_TASKS, 1);
        }
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
