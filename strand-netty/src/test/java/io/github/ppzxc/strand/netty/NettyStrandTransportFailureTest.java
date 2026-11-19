package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.OwnerBudgets;
import io.github.ppzxc.strand.netty.internal.ProgressSpace;
import io.netty.util.concurrent.Future;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** 전송이 준비 중에 끊기는 경우와 접수 예약의 반환. 실패는 준비 실패로 관찰되고 예약은 모두 돌아온다(#322). */
class NettyStrandTransportFailureTest {
    private AppFixture fixture;

    @AfterEach
    void cleanUp() {
        if (fixture != null) {
            fixture.close();
        }
    }

    /** 맺은 연결에서 첫 바이트를 받은 뒤 RST로 끊는 상대. */
    private static Thread resettingPeer(ServerSocket server) {
        Thread thread = new Thread(() -> {
            try (Socket socket = server.accept()) {
                socket.getInputStream().read();
                socket.setSoLinger(true, 0);
            } catch (IOException ignored) {
                // 시험 상대가 끝났다.
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    @Test
    void aPeerThatResetsTheConnectionDuringTheHelloIsAPreparationFailureAndTheSlotsComeBack() throws Exception {
        fixture = AppFixture.plain();
        NettyStrandClient client = fixture.clientBuilder().build();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            resettingPeer(server);

            Future<NettyStrandConnection> connecting =
                    client.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getLocalPort()));
            CompletableFuture<NettyStrandConnection> result = AppFixture.cf(connecting);

            try {
                result.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
                throw new AssertionError("연결이 성공했다");
            } catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(PreparationFailedException.class);
                PreparationFailure failure = ((PreparationFailedException) e.getCause()).failure();
                assertThat(failure.stage()).isEqualTo(PreparationStage.HELLO);
                assertThat(failure.cause()).isInstanceOf(IOException.class);
            }
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (client.resourceUsage(Resource.TOTAL_CONNECTIONS).current() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(client.resourceUsage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(client.resourceUsage(Resource.PREPARING_CONNECTIONS).current())
                .isZero();
    }

    @Test
    void anAdmissionThatCannotStartTheConnectionReturnsEveryReservation() {
        fixture = AppFixture.plain();
        OwnerBudgets owner = new OwnerBudgets(fixture.limits);
        ConnectionBudgets budgets = owner.newConnection(fixture.limits);
        ProgressSpace space = ProgressSpace.derive(8, 8, fixture.maxFrameSize, null);

        assertThat(ConnectionEngine.admit(budgets, space)).isNull();
        assertThat(owner.usage(Resource.TOTAL_CONNECTIONS).current()).isEqualTo(1);
        assertThat(owner.usage(Resource.PREPARING_CONNECTIONS).current()).isEqualTo(1);
        ConnectionEngine.cancelAdmission(budgets, space);

        assertThat(owner.usage(Resource.TOTAL_CONNECTIONS).current()).isZero();
        assertThat(owner.usage(Resource.PREPARING_CONNECTIONS).current()).isZero();
        assertThat(owner.usage(Resource.FRAME_ASSEMBLY_BYTES).current()).isZero();
        assertThat(CloseNotice.disconnected(null).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
    }
}
