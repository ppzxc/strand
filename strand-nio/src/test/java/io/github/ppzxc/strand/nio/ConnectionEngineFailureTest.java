package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.OwnerBudgets;
import io.github.ppzxc.strand.nio.internal.Passage;
import io.github.ppzxc.strand.nio.internal.ProgressSpace;
import io.github.ppzxc.strand.nio.internal.SerialExecutor;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 통로가 예기치 못한 오류를 내도 연결이 끝나고, 결과가 보존되며, 예약이 돌아오는지 본다. 통로를 직접 꽂아 구동기를 시험한다. */
class ConnectionEngineFailureTest {

    private AppFixture fixture;

    @AfterEach
    void tearDown() {
        if (fixture != null) {
            fixture.close();
        }
    }

    /** 읽을 것이 없고 쓰기는 지원하지 않는 통로. 구동기가 서문을 쓰려 하면 예상 못한 예외가 난다. */
    private static final class BrokenWritePassage implements Passage {
        @Override
        public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) {
            return Inbound.none();
        }

        @Override
        public int writeBytes(byte[] bytes, int offset, int length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void wakeup() {}

        @Override
        public void close() {}
    }

    private static final class Observed implements ConnectionParams.PreparationObserver {
        final CompletableFuture<PreparationFailure> failure = new CompletableFuture<>();

        @Override
        public void ready(NioStrandConnection connection) {}

        @Override
        public void failed(PreparationFailure value) {
            failure.complete(value);
        }
    }

    private NioStrandConnection connectionWith(
            ConnectionParams.PassageOpener opener, Observed observed, OwnerBudgets[] ownerOut) {
        fixture = AppFixture.plain();
        ResourceLimits limits = fixture.limits;
        OwnerBudgets owner = new OwnerBudgets(limits);
        ownerOut[0] = owner;
        ConnectionBudgets budgets = owner.newConnection(limits);
        ProgressSpace space = ProgressSpace.derive(8, 8, EndpointConfig.DEFAULT_MAX_FRAME_SIZE, null);
        assertThat(ConnectionEngine.admit(budgets, space)).isNull();
        ConnectionParams params = new ConnectionParams(
                ConnectionParams.Role.CLIENT,
                EndpointConfig.defaults().maxConcurrentStreams(8),
                opener,
                new InetSocketAddress("127.0.0.1", 1),
                new SerialExecutor(fixture.executor),
                fixture.executor,
                budgets,
                space,
                EndpointConfig.DEFAULT_MAX_FRAME_SIZE,
                System.nanoTime() + TimeUnit.SECONDS.toNanos(10),
                TimeUnit.SECONDS.toNanos(5),
                64L << 20,
                System::nanoTime,
                observed,
                null,
                null,
                null,
                false);
        NioStrandConnection connection = new NioStrandConnection(params);
        connection.start();
        return connection;
    }

    @Test
    void anUnexpectedErrorFromThePassageEndsTheConnectionAndKeepsTheCauseAsTheClosedResult() throws Exception {
        Observed observed = new Observed();
        OwnerBudgets[] owner = new OwnerBudgets[1];

        NioStrandConnection connection = connectionWith(remaining -> new BrokenWritePassage(), observed, owner);

        PreparationFailure failure = observed.failure.get(15, TimeUnit.SECONDS);
        CloseNotice notice = fixture.result(connection.closed());
        assertThat(failure.cause()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).containsSame(failure.cause());
        assertThat(owner[0].usage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(owner[0].usage(Resource.PREPARING_CONNECTIONS).current()).isZero();
    }

    @Test
    void anUnexpectedErrorWhileOpeningThePassageIsAPreparationFailureAtTheCurrentStage() throws Exception {
        Observed observed = new Observed();
        OwnerBudgets[] owner = new OwnerBudgets[1];
        IllegalStateException boom = new IllegalStateException("opener bug");

        NioStrandConnection connection = connectionWith(
                remaining -> {
                    throw boom;
                },
                observed,
                owner);

        PreparationFailure failure = observed.failure.get(15, TimeUnit.SECONDS);
        assertThat(failure.stage()).isEqualTo(PreparationStage.CONNECTION);
        assertThat(failure.cause()).isSameAs(boom);
        assertThat(fixture.result(connection.closed()).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
    }

    @Test
    void anIoErrorWhileOpeningThePassageIsAPreparationFailureToo() throws Exception {
        Observed observed = new Observed();
        OwnerBudgets[] owner = new OwnerBudgets[1];

        NioStrandConnection connection = connectionWith(
                remaining -> {
                    throw new IOException("no route");
                },
                observed,
                owner);

        assertThat(observed.failure.get(15, TimeUnit.SECONDS).cause()).isInstanceOf(IOException.class);
        assertThat(fixture.result(connection.closed()).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(owner[0].usage(Resource.TOTAL_CONNECTIONS).current()).isZero();
    }
}
