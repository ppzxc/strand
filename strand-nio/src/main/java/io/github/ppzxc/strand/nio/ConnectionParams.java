package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.function.LongSupplier;
import org.jspecify.annotations.Nullable;

/** 연결 하나를 만드는 데 필요한 설정과 이미 예약한 자원. 클라이언트와 서버 builder가 채운다. */
final class ConnectionParams {
    enum Role {
        CLIENT,
        SERVER
    }

    /** 연결 스레드가 통로를 연다. 클라이언트는 TCP 연결과 TLS를, 서버는 접수한 소켓의 통로 구성을 한다. */
    interface PassageOpener {
        /**
         * 준비 기한까지 남은 시간 안에 통로를 연다.
         *
         * @param remainingNanos 준비 기한까지 남은 나노초. 0 이하일 수 있다
         * @return 연 통로
         * @throws IOException 연결하지 못했을 때
         */
        Passage open(long remainingNanos) throws IOException;
    }

    /** 준비 결과(성공·실패)를 소유 객체에 알린다. 호출은 연결의 직렬 실행기에서 한다. */
    interface PreparationObserver {
        void ready(NioStrandConnection connection);

        void failed(PreparationFailure failure);
    }

    final Role role;
    final EndpointConfig config;
    final PassageOpener opener;
    final SocketAddress remoteAddress;
    final SerialExecutor executor;
    /** 앱이 준 실행기. 준비 결과 알림은 연결별 직렬 실행을 거치지 않는다(반환하지 않는 사용자 작업 뒤에 막히지 않게). */
    final java.util.concurrent.Executor appExecutor;

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
    final boolean tls;

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
