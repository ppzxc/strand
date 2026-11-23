package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * CLOSED 뒤 마무리(남은 쓰기, 상대가 끊을 때까지 읽기)에 시간 상한이 있는지 본다. 구 연결 드라이버의 CLOSED 뒤 마무리 시험을 제품 엔진 위로 옮긴 것이다.
 * 수동 시계와 메모리 통로를 쓰고, 상대(코어 엔드포인트)는 먼저 끊지 않는 한 끊지 않는다. 전송 스레드가 끝났는지는 종료 알림이 확정됐는지로 본다.
 */
class EngineClosedFinishTest {
    private static final long LIMIT_MS = 5000;
    private static final long ACK_TIMEOUT_MS = 1000;
    private static final long IDLE_INTERVAL_MS = 3000;
    private static final long UNRESPONSIVE_ERROR_CODE = 6;
    private static final long PLENTY = 1_000_000;

    private EngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private static EngineRig.Tuning tuning() {
        return new EngineRig.Tuning().config(config -> config.ackTimeoutMs(ACK_TIMEOUT_MS)
                .idleIntervalMs(IDLE_INTERVAL_MS));
    }

    private void tick(long ms) {
        rig.advance(Duration.ofMillis(ms));
        rig.settle();
    }

    private boolean alive() {
        return !rig.connection.closed().isDone();
    }

    private CloseNotice ended() throws Exception {
        return rig.fixture.result(rig.connection.closed());
    }

    @Test
    void afterClosedTheConnectionEndsAtTheDefaultLimitEvenIfThePeerNeverHangsUp() throws Exception {
        rig = EngineRig.client(tuning());
        rig.connection.close();
        rig.settle();
        assertThat(alive()).isTrue();

        tick(LIMIT_MS - 1);
        assertThat(alive()).isTrue();

        tick(1);
        CloseNotice notice = ended();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
    }

    @Test
    void theLimitCountsFromTheMomentClosedWasProducedAndNotFromWhenTheEngineStarted() throws Exception {
        rig = EngineRig.client(tuning());
        tick(LIMIT_MS * 2);
        rig.connection.close();
        rig.settle();
        tick(LIMIT_MS - 1);
        assertThat(alive()).isTrue();

        tick(1);
        ended();
    }

    @Test
    void theLimitFollowsTheConfiguredCloseFinishDeadline() throws Exception {
        rig = EngineRig.client(tuning().closeFinish(Duration.ofMillis(200)));
        rig.connection.close();
        rig.settle();
        tick(199);
        assertThat(alive()).isTrue();

        tick(1);
        ended();
    }

    @Test
    void ifTheWriteStaysBlockedAfterAnErrorCloseTheSameLimitEndsTheConnection() throws Exception {
        rig = EngineRig.client(tuning());
        rig.client.writeCapacity(0);
        // 막힌 쓰기 뒤라 유휴 PING을 넘기지 못한다. 보내기로 한 때부터 확인 대기 시간이 지나면 확인 대기 초과와 같게 닫는다.
        tick(IDLE_INTERVAL_MS);
        tick(ACK_TIMEOUT_MS + 1);
        assertThat(alive()).isTrue();

        tick(LIMIT_MS - 1);
        assertThat(alive()).isTrue();

        tick(1);
        CloseNotice notice = ended();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
    }

    @Test
    void ifThePeersCloseIsRepliedToWhileTheWriteIsBlockedTheSameLimitEndsTheConnection() throws Exception {
        rig = EngineRig.client(tuning());
        rig.client.writeCapacity(0);
        rig.peer.close(0);
        rig.settle();
        assertThat(alive()).isTrue();

        tick(LIMIT_MS - 1);
        assertThat(alive()).isTrue();

        tick(1);
        CloseNotice notice = ended();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.sender()).contains(CloseNotice.Sender.PEER);
    }

    @Test
    void aVeryLargeLimitDoesNotOverflowAndTheWaitStaysWithinTheIntRange() throws Exception {
        EngineRig.Tuning tuning = tuning();
        tuning.closeFinishNanos = Long.MAX_VALUE;
        rig = EngineRig.client(tuning);
        rig.connection.close();
        rig.settle();
        assertThat(rig.client.lastAwaitTimeoutMs()).isEqualTo(Integer.MAX_VALUE);

        tick(LIMIT_MS * 10);
        assertThat(alive()).isTrue();
        assertThat(rig.client.lastAwaitTimeoutMs()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void theWaitAfterClosedIsBoundedByTheRemainingLimit() {
        rig = clientClosed();
        assertThat(rig.client.lastAwaitTimeoutMs()).isBetween(LIMIT_MS, LIMIT_MS + 1);

        // 시계만 가고 깨우지 않아도 다음 기다림은 남은 시간까지다.
        tick(1000);
        assertThat(rig.client.lastAwaitTimeoutMs()).isBetween(LIMIT_MS - 1000, LIMIT_MS - 1000 + 1);
    }

    private static EngineRig clientClosed() {
        try {
            EngineRig value = EngineRig.client(tuning());
            value.connection.close();
            value.settle();
            return value;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void ifThePeerHangsUpBeforeTheLimitTheConnectionEndsAtOnce() throws Exception {
        rig = EngineRig.client(tuning());
        rig.connection.close();
        rig.settle();
        assertThat(alive()).isTrue();

        rig.peerHangsUp();

        assertThat(ended().kind()).isEqualTo(CloseNotice.Kind.CLOSE);
    }

    @Test
    void aBlockedWriteThatClearsBeforeTheLimitStillLetsTheConnectionEndOnThePeersHangUp() throws Exception {
        rig = EngineRig.client(tuning());
        rig.client.writeCapacity(0);
        tick(IDLE_INTERVAL_MS);
        tick(ACK_TIMEOUT_MS + 1);
        assertThat(alive()).isTrue();

        rig.client.addWriteCapacity(PLENTY);
        rig.peerHangsUp();

        CloseNotice notice = ended();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
    }
}
