package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * CLOSED 뒤 마무리(남은 쓰기, 상대가 끊을 때까지 읽기)에 시간 상한이 있는지 본다. nio의 {@code EngineClosedFinishTest}에 대응한다. 수동 시계와 실제 채널을 쓰고, 상대(코어
 * 엔드포인트)는 먼저 끊지 않는 한 끊지 않는다. 엔진이 CLOSED를 내고 보내는 쪽을 닫았는지는 상대 소켓이 끝을 읽었는지로 알고(그때부터 마무리 상한이 센다), 연결이 끝났는지는 종료 알림이
 * 확정됐는지로 본다. 쓰기가 막힌 경우는 상대의 CLOSE 바이트를 엔진 입력에 직접 넣어 CLOSED를 내는 걸음을 결정한다.
 */
class NettyEngineClosedFinishTest {
    private static final long LIMIT_MS = 5000;
    private static final long ACK_TIMEOUT_MS = 1000;
    private static final long IDLE_INTERVAL_MS = 3000;
    private static final long UNRESPONSIVE_ERROR_CODE = 6;
    private static final long PLENTY = 1_000_000;

    private NettyEngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private static NettyEngineRig.Tuning tuning() {
        return new NettyEngineRig.Tuning()
                .config(config -> config.ackTimeoutMs(ACK_TIMEOUT_MS).idleIntervalMs(IDLE_INTERVAL_MS));
    }

    private CloseNotice ended() throws Exception {
        return rig.fixture.result(rig.connection.closed());
    }

    /** 엔진이 CLOSE를 보내고 상대의 CLOSE로 CLOSED를 낸 뒤 보내는 쪽을 닫았음을 기다린다. */
    private void closeAndAwaitClosed() throws Exception {
        rig.connection.close();
        NettyEngineRig.awaitTrue(rig.peer::ended);
    }

    @Test
    void afterClosedTheConnectionEndsAtTheDefaultLimitEvenIfThePeerNeverHangsUp() throws Exception {
        rig = NettyEngineRig.client(tuning());
        closeAndAwaitClosed();
        assertThat(rig.closed()).isFalse();

        rig.tick(LIMIT_MS - 1);
        assertThat(rig.closed()).isFalse();

        rig.tick(1);
        assertThat(ended().kind()).isEqualTo(CloseNotice.Kind.CLOSE);
    }

    @Test
    void theLimitCountsFromTheMomentClosedWasProducedAndNotFromWhenTheEngineStarted() throws Exception {
        rig = NettyEngineRig.client(tuning());
        rig.tick(LIMIT_MS * 2);
        closeAndAwaitClosed();
        rig.tick(LIMIT_MS - 1);
        assertThat(rig.closed()).isFalse();

        rig.tick(1);
        ended();
    }

    @Test
    void theLimitFollowsTheConfiguredCloseFinishDeadline() throws Exception {
        rig = NettyEngineRig.client(tuning().closeFinish(Duration.ofMillis(200)));
        closeAndAwaitClosed();
        rig.tick(199);
        assertThat(rig.closed()).isFalse();

        rig.tick(1);
        ended();
    }

    @Test
    void ifTheWriteStaysBlockedAfterAnErrorCloseTheSameLimitEndsTheConnection() throws Exception {
        rig = NettyEngineRig.client(tuning());
        rig.channel.capacity(0);
        // 막힌 쓰기 뒤라 유휴 PING을 넘기지 못한다. 보내기로 한 때부터 확인 대기 시간이 지나면 확인 대기 초과와 같게 닫는다.
        rig.tick(IDLE_INTERVAL_MS);
        rig.tick(ACK_TIMEOUT_MS + 1);
        assertThat(rig.closed()).isFalse();

        rig.tick(LIMIT_MS - 1);
        assertThat(rig.closed()).isFalse();

        rig.tick(1);
        CloseNotice notice = ended();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
    }

    @Test
    void ifThePeersCloseIsRepliedToWhileTheWriteIsBlockedTheSameLimitEndsTheConnection() throws Exception {
        rig = NettyEngineRig.client(tuning());
        rig.channel.capacity(0);
        rig.inject(rig.peer.runDetached(endpoint -> endpoint.close(0)));
        assertThat(rig.closed()).isFalse();

        rig.tick(LIMIT_MS - 1);
        assertThat(rig.closed()).isFalse();

        rig.tick(1);
        CloseNotice notice = ended();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.sender()).contains(CloseNotice.Sender.PEER);
    }

    @Test
    void aVeryLargeLimitDoesNotOverflowAndTheConnectionStaysUntilThePeerHangsUp() throws Exception {
        NettyEngineRig.Tuning tuning = tuning();
        tuning.closeFinishNanos = Long.MAX_VALUE;
        rig = NettyEngineRig.client(tuning);
        closeAndAwaitClosed();

        // 끝없는 상한이 넘쳐 음수 기한이 되면 연결이 곧바로 끝난다.
        rig.tick(LIMIT_MS * 10);
        assertThat(rig.closed()).isFalse();

        rig.peerHangsUp();
        assertThat(ended().kind()).isEqualTo(CloseNotice.Kind.CLOSE);
    }

    @Test
    void ifThePeerHangsUpBeforeTheLimitTheConnectionEndsAtOnce() throws Exception {
        rig = NettyEngineRig.client(tuning());
        closeAndAwaitClosed();
        assertThat(rig.closed()).isFalse();

        rig.peerHangsUp();

        assertThat(ended().kind()).isEqualTo(CloseNotice.Kind.CLOSE);
    }

    @Test
    void aBlockedWriteThatClearsBeforeTheLimitStillLetsTheConnectionEndOnThePeersHangUp() throws Exception {
        rig = NettyEngineRig.client(tuning());
        rig.channel.capacity(0);
        rig.tick(IDLE_INTERVAL_MS);
        rig.tick(ACK_TIMEOUT_MS + 1);
        assertThat(rig.closed()).isFalse();

        rig.channel.addCapacity(PLENTY);
        rig.peerHangsUp();

        CloseNotice notice = ended();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
    }
}
