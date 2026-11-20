package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceExhaustedException;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.OwnerBudgets;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 주소 해석 결과·기한·취소에 따른 TcpConnector의 분기를 수동 시계와 닫힌 로컬 포트로 결정적으로 확인한다. */
class TcpConnectorBranchTest {
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    private AppFixture fixture = AppFixture.plain();

    @AfterEach
    void tearDown() {
        fixture.close();
    }

    private static int closedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private final OwnerBudgets owner = new OwnerBudgets(fixture.limits);
    private final ConnectionBudgets budgets = owner.newConnection(fixture.limits);
    private final ConnectAttemptState state = new ConnectAttemptState();

    private TcpConnector connector(InetSocketAddress address, TcpConnector.Resolver resolver, LongSupplier clock) {
        return new TcpConnector(
                budgets, address, null, resolver, fixture.executor, state, new PreparationBytes(budgets), clock);
    }

    private static InetSocketAddress unresolved(int port) {
        return InetSocketAddress.createUnresolved("name.invalid", port);
    }

    @Test
    void anEmptyResolutionFailsAsUnknownHost() {
        TcpConnector connector = connector(unresolved(1), host -> new InetAddress[0], System::nanoTime);

        assertThatThrownBy(() -> connector.open(10 * SECOND)).isInstanceOf(UnknownHostException.class);
    }

    @Test
    void aFailedTargetFallsThroughToTheNextAndThenReportsTheLastFailure() throws Exception {
        int port = closedPort();
        InetAddress loopback = InetAddress.getLoopbackAddress();
        TcpConnector connector =
                connector(unresolved(port), host -> new InetAddress[] {loopback, loopback}, System::nanoTime);

        assertThatThrownBy(() -> connector.open(10 * SECOND)).isInstanceOf(IOException.class);
        assertThat(state.cancelRequested()).isFalse();
    }

    @Test
    void aFailureAfterACancelIsRethrownInsteadOfBeingRememberedForTheNextTarget() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        TcpConnector connector =
                connector(new InetSocketAddress(loopback, closedPort()), host -> new InetAddress[0], System::nanoTime);
        // 취소가 먼저 접수되면 통로 열기가 시작되자마자 채널이 닫혀 연결이 입출력 예외로 끝난다.
        assertThat(state.cancel()).isTrue();

        assertThatThrownBy(() -> connector.open(10 * SECOND)).isInstanceOf(IOException.class);
    }

    @Test
    void anExhaustedResolveSlotFailsWithResourceExhausted() {
        long limit = owner.usage(Resource.RESOLVE_TASKS).limit();
        assertThat(owner.tryReserve(Resource.RESOLVE_TASKS, limit)).isTrue();
        TcpConnector connector = connector(unresolved(1), host -> new InetAddress[0], System::nanoTime);

        assertThatThrownBy(() -> connector.open(10 * SECOND))
                .isInstanceOf(IOException.class)
                .hasCauseInstanceOf(ResourceExhaustedException.class);
    }

    @Test
    void aDeadlinePassedWhileWaitingForResolutionTimesOutWithoutWaiting() {
        AtomicLong calls = new AtomicLong();
        // 첫 호출은 기한을 정하고, 이후 호출은 기한이 이미 지난 시각을 준다.
        LongSupplier clock = () -> calls.getAndIncrement() == 0 ? 0 : 100 * SECOND;
        TcpConnector connector = connector(unresolved(1), host -> new InetAddress[0], clock);

        assertThatThrownBy(() -> connector.open(SECOND)).isInstanceOf(SocketTimeoutException.class);
    }

    @Test
    void aRuntimeFailureOfTheResolverSurfacesUnchanged() {
        IllegalStateException failure = new IllegalStateException("해석 실패");
        TcpConnector connector = connector(
                unresolved(1),
                host -> {
                    throw failure;
                },
                System::nanoTime);

        assertThatThrownBy(() -> connector.open(10 * SECOND)).isSameAs(failure);
    }

    @Test
    void anErrorFromTheResolverSurfacesUnchanged() {
        AssertionError failure = new AssertionError("해석 오류");
        TcpConnector connector = connector(
                unresolved(1),
                host -> {
                    throw failure;
                },
                System::nanoTime);

        assertThatThrownBy(() -> connector.open(10 * SECOND)).isSameAs(failure);
    }

    @Test
    void aCheckedFailureOfTheResolverIsWrappedInAnIoException() {
        Exception failure = new TimeoutException("해석 지연");
        TcpConnector connector = connector(
                unresolved(1),
                host -> {
                    TcpConnectorBranchTest.<RuntimeException>sneakyThrow(failure);
                    return new InetAddress[0];
                },
                System::nanoTime);

        assertThatThrownBy(() -> connector.open(10 * SECOND))
                .isInstanceOf(IOException.class)
                .hasCause(failure);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }
}
