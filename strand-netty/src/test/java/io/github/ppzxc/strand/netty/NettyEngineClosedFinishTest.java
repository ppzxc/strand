package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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

    private void closeAndAwaitClosed() throws Exception {
        rig.connection.close();
        NettyRig.awaitTrue(rig.peer::ended);
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
