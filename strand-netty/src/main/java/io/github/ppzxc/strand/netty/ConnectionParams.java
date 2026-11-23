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

/** 연결 하나를 만드는 데 필요한 설정과 이미 예약한 자원. 클라이언트와 서버 builder가 채운다. */
final class ConnectionParams {
    enum Role {
        CLIENT,
        SERVER
    }

    /** 준비 결과(성공·실패)를 소유 객체에 알린다. 호출은 앱용 실행기에서 한다. */
    interface PreparationObserver {
        void ready(NettyStrandConnection connection);

        void failed(PreparationFailure failure);

        /**
         * 앱용 실행기가 준비 결과 알림을 거절했다. 알림을 대신할 스레드를 만들지 않으므로 이 호출은 EventLoop에서 오며 사용자 코드를 실행하면 안 된다. 성공이었다면
         * 연결은 곧 끊긴다. 기본은 실패 알림으로 본다.
         *
         * @param failure 앱에 닿지 못한 결과(성공이 거절됐다면 거절 원인)
         */
        default void rejected(PreparationFailure failure) {
            failed(failure);
        }
    }

    /** 채널 보안으로 TLS를 쓸 때의 구성: 컨텍스트, 엔진 버퍼 크기, 클라이언트가 검증할 서버 이름. */
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
    /** 이 연결의 채널이 고정되는 EventLoop. core는 이 EventLoop에서만 부른다. */
    final EventLoop loop;

    final SerialExecutor executor;
    /** 앱이 준 실행기. 준비 결과 알림은 연결별 직렬 실행을 거치지 않는다(반환하지 않는 사용자 작업 뒤에 막히지 않게). */
    final Executor appExecutor;

    final ConnectionBudgets budgets;
    final ProgressSpace space;
    final long maxFrameSize;
    /** 준비 기한의 절대 시각({@link #nanoClock} 기준). */
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
    /** 클라이언트 연결 시도의 취소 상태. 서버 연결에는 없다. */
    @Nullable
    ConnectAttemptState attempt;
    /** 라이브러리가 보관한 준비 데이터의 바이트 예약. 클라이언트 연결에만 있다. */
    @Nullable
    PreparationBytes preparationBytes;
    /** 연결이 모든 정리를 마친 맨 마지막에 한 번 부른다. 소유 객체가 전체 종료를 위해 연결을 기록에서 뺀다. */
    volatile Runnable terminated = () -> {};
    /** 시도가 소유한 채널 정리 완료를 알린다. 클라이언트 연결에만 있다. */
    io.netty.util.concurrent.@Nullable Promise<Void> released;
    /** 활성화되지 못해 폐기 실행이 거절된 팩터리 결과를 보존하는 곳. 서버 연결에만 있다. */
    @Nullable
    UnreleasedHandlers unreleased;
    /** 클라이언트가 호스트 이름을 주소 목록으로 해석하는 방법. */
    HostResolver resolver = HostResolver.SYSTEM;
    /** 이 연결의 전송 방식. 기본은 TCP다. */
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
