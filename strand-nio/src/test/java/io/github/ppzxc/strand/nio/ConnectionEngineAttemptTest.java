package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.InMemoryPassage;
import io.github.ppzxc.strand.nio.internal.OwnerBudgets;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 연결 시도 취소 상태와 엔진의 만남: 상대가 응답하지 않는 준비 중 연결을 취소·전체 종료로 끝낸다. */
class ConnectionEngineAttemptTest {

    private final AppFixture fixture = AppFixture.plain();
    private final ConnectAttemptState attempt = new ConnectAttemptState();
    private final CompletableFuture<PreparationFailure> failure = new CompletableFuture<>();

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    /** 상대가 아무것도 보내지 않는 메모리 통로 위의 준비 중 연결. 시작하지 않은 채 돌려준다. */
    private NioStrandConnection silentPeerConnection() {
        OwnerBudgets owner = new OwnerBudgets(fixture.limits);
        ConnectionBudgets budgets = owner.newConnection(fixture.limits);
        ProgressSpace space = ProgressSpace.derive(8, 8, EndpointConfig.DEFAULT_MAX_FRAME_SIZE, null);
        assertThat(ConnectionEngine.admit(budgets, space)).isNull();
        InMemoryPassage passage = InMemoryPassage.streams().first();
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.CLIENT,
                EndpointConfig.defaults().maxConcurrentStreams(8),
                remaining -> passage,
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 1),
                new SerialExecutor(fixture.executor),
                fixture.executor,
                budgets,
                space,
                EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                System.nanoTime() + TimeUnit.MINUTES.toNanos(5),
                TimeUnit.SECONDS.toNanos(5),
                64L << 20,
                System::nanoTime,
                new ConnectionParams.PreparationObserver() {
                    @Override
                    public void ready(NioStrandConnection connection) {}

                    @Override
                    public void failed(PreparationFailure value) {
                        failure.complete(value);
                    }
                },
                null,
                null,
                null,
                false);
        params.attempt = attempt;
        return new NioStrandConnection(params);
    }

    @Test
    void aCancelAcceptedBeforeTheEngineStartsIsDeliveredWhenTheEngineIsBound() throws Exception {
        NioStrandConnection connection = silentPeerConnection();
        assertThat(attempt.cancel()).isTrue();

        connection.start();

        assertThat(failure.get(15, TimeUnit.SECONDS).cause()).isInstanceOf(ConnectionAttemptCancelledException.class);
        connection.abort();
    }

    @Test
    void anOwnerShutdownCloseCancelsThePreparingAttempt() throws Exception {
        NioStrandConnection connection = silentPeerConnection();
        connection.start();

        connection.shutdownClose();

        assertThat(attempt.cancelRequested()).isTrue();
        assertThat(failure.get(15, TimeUnit.SECONDS).cause()).isInstanceOf(ConnectionAttemptCancelledException.class);
        connection.abort();
    }

    @Test
    void anOwnerShutdownAbortCancelsThePreparingAttempt() throws Exception {
        NioStrandConnection connection = silentPeerConnection();
        connection.start();

        connection.shutdownAbort();

        assertThat(attempt.cancelRequested()).isTrue();
        assertThat(failure.get(15, TimeUnit.SECONDS).cause()).isNotNull();
        connection.abort();
    }
}
