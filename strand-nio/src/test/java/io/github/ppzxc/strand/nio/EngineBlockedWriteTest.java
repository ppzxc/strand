package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.Status;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 통로 쓰기가 막혀도(상대가 읽지 않아 쓰기 용량이 0) 전송 스레드가 읽기, 시계, 명령 처리를 계속 도는지 본다. 구 연결 드라이버의 쓰기 막힘 시험을 제품 엔진 위로
 * 옮긴 것이다. 수동 시계와 용량을 시험이 정하는 메모리 통로를 쓰고, 쓰기 막힘은 용량 0으로 만든다. 연결이 끝난 것은 종료 알림 확정(전송 스레드 종료)으로 보고,
 * 종료 알림이 나온 뒤의 마무리 상한은 100밀리초로 줄여 시계를 몇 번 올려 가르는 방식으로 때를 잰다.
 */
class EngineBlockedWriteTest {
    private static final long ACK_TIMEOUT_MS = 1000;
    private static final long IDLE_INTERVAL_MS = 3000;
    private static final long FINISH_MS = 100;
    private static final long UNRESPONSIVE_ERROR_CODE = 6;
    private static final long PLENTY = 1_000_000;
    private static final byte[] BODY = new byte[50];

    private EngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private static EngineRig.Tuning tuning() {
        return new EngineRig.Tuning()
                .config(config -> config.ackTimeoutMs(ACK_TIMEOUT_MS).idleIntervalMs(IDLE_INTERVAL_MS))
                .closeFinish(Duration.ofMillis(FINISH_MS));
    }

    private RequestHandle send() {
        return rig.connection.request(Request.of(EngineRig.TYPE, BODY));
    }

    private void tick(long ms) {
        rig.advance(Duration.ofMillis(ms));
        rig.settle();
    }

    /** 확인 대기 초과가 정확히 기한(+1ms)에 판정되는지 본다: 판정 전에는 마무리 상한이 시작되지 않아 일찍 끝나지 않고, 판정 뒤 상한이 차야 끝난다. */
    private void assertEndsAsUnresponsiveRightAfterTheAckWait() throws Exception {
        tick(IDLE_INTERVAL_MS);
        tick(ACK_TIMEOUT_MS);
        tick(1);
        tick(FINISH_MS - 1);
        assertThat(rig.connection.closed().isDone()).isFalse();
        tick(1);
        assertThat(rig.connection.closed().isDone()).isTrue();
        CloseNotice notice = rig.connection.closed().resultNow();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
    }

    @Test
    void aPeerThatDoesNotReadEndsTheConnectionWhenTheAckWaitRunsOut() throws Exception {
        rig = EngineRig.client(tuning());
        // 요청의 앞 10바이트만 나가고 막힌다. 쓰기를 시작한 요청이다.
        rig.client.writeCapacity(10);
        RequestHandle handle = send();
        rig.settle();

        assertEndsAsUnresponsiveRightAfterTheAckWait();

        assertThat(handle.result().isDone()).isTrue();
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    @Test
    void theIdlePingIsDueOnScheduleWhileTheWriteIsBlocked() throws Exception {
        rig = EngineRig.client(tuning());
        rig.client.writeCapacity(0);

        // 유휴 PING이 제때 나가야(꺼내져 확인 대기가 돌아야) 그 확인 대기가 PING 시점부터 센다.
        assertEndsAsUnresponsiveRightAfterTheAckWait();
    }

    @Test
    void aRequestDeadlineStillPassesWhileTheWriteIsBlockedAfterAPartialWrite() throws Exception {
        rig = EngineRig.client(tuning());
        rig.client.writeCapacity(10);
        RequestHandle handle = rig.connection.request(
                Request.of(EngineRig.TYPE, BODY), RequestOptions.deadlineAfter(Duration.ofMillis(100)));
        rig.settle();

        tick(100);
        assertThat(handle.result().isDone()).isFalse();

        tick(5);
        assertThat(handle.result().isDone()).isTrue();
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    @Test
    void aRequestDeadlineThatPassesBeforeAnyByteWasWrittenIsNotProcessed() throws Exception {
        rig = EngineRig.client(tuning());
        rig.client.writeCapacity(0);
        RequestHandle handle = rig.connection.request(
                Request.of(EngineRig.TYPE, BODY), RequestOptions.deadlineAfter(Duration.ofMillis(100)));
        rig.settle();

        tick(105);

        assertThat(handle.result().isDone()).isTrue();
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.NOT_PROCESSED);
    }

    @Test
    void timeThatFlowedWhileBlockedIsNotAdvancedAgainWhenTheBlockClears() throws Exception {
        rig = EngineRig.client(tuning());
        rig.client.writeCapacity(0);
        RequestHandle handle = send();
        rig.settle();
        tick(ACK_TIMEOUT_MS - 400);

        rig.client.addWriteCapacity(PLENTY);
        rig.settle();
        // 상대가 요청을 받아 확인했다. 풀린 뒤 시간이 다시 몰려 흐르지 않으니 연결이 그대로다.
        tick(ACK_TIMEOUT_MS - 400);

        assertThat(rig.connection.closed().isDone()).isFalse();
        assertThat(rig.peerRequestStreams()).hasSize(1);
        rig.peerRespond(rig.peerRequestStreams().get(0), 0, new byte[] {1});
        assertThat(handle.result().isDone()).isTrue();
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.RESPONSE);
    }

    @Test
    void thePeersMessagesAreStillReadWhileTheWriteIsBlocked() throws Exception {
        List<IncomingRequest> received = new CopyOnWriteArrayList<>();
        rig = EngineRig.server(received::add, tuning());
        rig.client.writeCapacity(0);

        rig.peer.request(EngineRig.TYPE, new byte[] {1});
        rig.settle();

        // 쓰기가 막혀 응답은 못 내지만 상대의 요청은 읽혀 핸들러에 닿는다.
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (received.isEmpty() && System.nanoTime() < limit) {
            Thread.sleep(5);
        }
        assertThat(received).hasSize(1);
    }

    @Test
    void commandsFromOtherThreadsAreHandledWhileTheWriteIsBlocked() throws Exception {
        rig = EngineRig.client(tuning());
        rig.client.writeCapacity(0);

        // 요청과 종료 시작은 명령으로 처리되고, 막힌 쓰기와 상관없이 abort가 연결을 끝낸다.
        RequestHandle pending = send();
        rig.settle();
        rig.connection.close();
        rig.settle();
        assertThat(pending.result().isDone()).isFalse();

        rig.connection.abort();
        rig.settle();
        assertThat(pending.result().isDone()).isTrue();
        assertThat(rig.fixture.result(rig.connection.closed()).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
    }

    @Test
    void aWriteFailureWhileBlockedEndsTheConnectionAndTheWaitingRequest() throws Exception {
        rig = EngineRig.client(tuning());
        rig.client.writeCapacity(10);
        RequestHandle handle = send();
        rig.settle();

        // 출력을 닫은 통로에 쓰기 가능이 오면 다음 쓰기가 입출력 오류로 끝난다.
        rig.client.shutdownOutput();
        rig.client.addWriteCapacity(PLENTY);
        rig.settle();

        assertThat(handle.result().isDone()).isTrue();
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.UNAVAILABLE);
        // 요청 프레임을 끝까지 쓰지 못했으므로 상대는 처리하지 않았다.
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(rig.fixture.result(rig.connection.closed()).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
    }
}
