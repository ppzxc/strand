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

/**
 * NIO 서버. {@link #listen}은 바인딩이 끝나면 성공한다. 연결마다 TCP 수락부터 준비 기한을 세고, 개별 연결의 준비 실패는 리스닝 성공을 바꾸거나 중단하지
 * 않고 {@code onConnectionFailure}로 관찰한다. 이 객체는 앱이 준 실행기를 종료하지 않는다.
 */
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

    /**
     * builder를 만든다.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** {@link NioStrandServer} builder. */
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
         * <p>앱용 실행기가 이 알림의 제출을 거절하면 알림을 버리고 이 함수를 호출하지 않는다. 대신 실행할 스레드를 만들지 않는다. 준비 성공 알림의 제출이 거절돼도 연결은 유지되며 요청을 처리한다(서버에는 성공을 알릴 앱 관찰자가 없다).
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
        public NioStrandServer build() {
            if (handler != null && factory != null) {
                throw new IllegalArgumentException("handler와 handlerFactory는 함께 쓸 수 없다");
            }
            Common common = validate();
            return new NioStrandServer(common, Objects.requireNonNull(verifier), handler, factory, failureListener);
        }
    }

    /**
     * 주소에 바인딩해 연결 수락을 시작한다. 성공은 바인딩 완료다. 바인딩 실패는 Stage의 비동기 실패다.
     *
     * @param address 바인딩할 주소
     * @return 리스닝 결과
     */
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
                Listener listener = new Listener(server, server.getLocalAddress(), this::admit, common.executor);
                if (lifecycle.register(listener)) {
                    future.complete(listener);
                } else {
                    // 바인딩하는 사이 전체 종료가 시작됐다. 리스닝을 닫고 실패시킨다.
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
            // 닫는 중의 오류는 할 수 있는 일이 없다.
        }
    }

    /** 수락한 소켓을 연결로 등록한다. 슬롯과 필수 진행 공간을 수락 직후 예약하고 모자라면 소켓을 즉시 닫는다. */
    private void admit(SocketChannel channel) {
        long now = common.nanoClock.getAsLong();
        SocketAddress remote;
        try {
            // 수락한 소켓은 이미 맺어져 있으므로 상대 주소가 있다. 이미 끊겼으면 읽을 수 없다.
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
                    public void rejected(PreparationFailure failure) {
                        // 앱용 실행기가 알림을 거절했다. 전송 스레드에서 사용자 코드(관찰자)를 대신 실행하지 않고 관찰할 수 없는 실패는 버린다.
                    }
                },
                handler,
                factory,
                verifier,
                common.security.tlsContext() != null);
        params.unreleased = unreleased;
        NioStrandConnection connection = new NioStrandConnection(params);
        if (!lifecycle.register(connection)) {
            // 전체 종료가 시작됐다. 새 접수를 받지 않으므로 예약을 돌려주고 소켓을 닫는다.
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

    /**
     * 소유 객체 전체를 종료한다. 새 접수를 막고, 진행 중인 시도와 리스닝을 정리하고, 연결에 CLOSE를 시작하고, {@code shutdownGrace}가 끝나면 남은 연결을
     * 끊고, 내부 실행 자원을 종료한다. 기한은 첫 호출 시각부터 센다. 반복 호출은 같은 결과를 돌려주며 기한을 늘리지 않는다. 앱이 준 실행기는 종료하지 않는다.
     *
     * @return 모든 내부 자원이 {@code shutdownDeadline} 안에 정리되면 성공, 아니면 {@link
     *     io.github.ppzxc.strand.api.ShutdownDeadlineExceededException}으로 실패하는 결과
     */
    public CompletionStage<Void> shutdown() {
        return lifecycle.shutdown();
    }

    /**
     * 전체 종료 결과의 보존된 관찰. {@code COMPLETED} 또는 {@code DEADLINE_EXCEEDED}와 기한 시점에 정리되지 않은 자원을 담는다. 확정한 결과는 바꾸지 않는다.
     *
     * @return 종료 결과
     */
    public Result<ShutdownResult> shutdownResult() {
        return lifecycle.result();
    }

    /**
     * 실제 내부 자원(연결·리스닝)과 잔존 작업(핸들러·팩터리·인증·주소 해석)과 미정리 결과가 모두 해결되면 완료한다. 종료 기한 초과 뒤에도 정리가 계속되며, 그때 완료한다.
     * 앱이 준 실행기 자체의 종료는 기다리지 않는다.
     *
     * @return 실제 정리 완료
     */
    public CompletionStage<Void> resourcesTerminated() {
        return lifecycle.terminated();
    }
}
