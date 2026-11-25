package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.Status;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NettyEngineBlockedWriteTest {
    private static final long ACK_TIMEOUT_MS = 1000;
    private static final long IDLE_INTERVAL_MS = 3000;
    private static final long FINISH_MS = 100;
    private static final long UNRESPONSIVE_ERROR_CODE = 6;
    private static final long PLENTY = 1_000_000;
    private static final byte[] BODY = new byte[50];

    private NettyEngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private static NettyEngineRig.Tuning tuning() {
        return new NettyEngineRig.Tuning()
                .config(config -> config.ackTimeoutMs(ACK_TIMEOUT_MS).idleIntervalMs(IDLE_INTERVAL_MS))
                .closeFinish(Duration.ofMillis(FINISH_MS));
    }

    private RequestHandle send() {
        return rig.connection.request(Request.of(NettyEngineRig.TYPE, BODY));
    }

    private void assertEndsAsUnresponsiveRightAfterTheAckWait() throws Exception {
        rig.tick(IDLE_INTERVAL_MS);
        rig.tick(ACK_TIMEOUT_MS);
        rig.tick(1);
        rig.tick(FINISH_MS - 1);
        assertThat(rig.closed()).isFalse();
        rig.tick(1);
        CloseNotice notice = rig.fixture.result(rig.connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
    }

    @Test
    void aPeerThatDoesNotReadEndsTheConnectionWhenTheAckWaitRunsOut() throws Exception {
        rig = NettyEngineRig.client(tuning());
        rig.channel.capacity(10);
        RequestHandle handle = send();
        rig.awaitStalls(1);

        assertEndsAsUnresponsiveRightAfterTheAckWait();

        assertThat(rig.fixture.result(handle.result()).status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    @Test
    void theIdlePingIsDueOnScheduleWhileTheWriteIsBlocked() throws Exception {
        rig = NettyEngineRig.client(tuning());
        rig.channel.capacity(0);

        assertEndsAsUnresponsiveRightAfterTheAckWait();
    }

    @Test
    void aRequestDeadlineStillPassesWhileTheWriteIsBlockedAfterAPartialWrite() throws Exception {
        rig = NettyEngineRig.client(tuning());
        rig.channel.capacity(10);
        RequestHandle handle = rig.connection.request(
                Request.of(NettyEngineRig.TYPE, BODY), RequestOptions.deadlineAfter(Duration.ofMillis(100)));
        rig.awaitStalls(1);

        rig.tick(100);
        assertThat(handle.result().isDone()).isFalse();

        rig.tick(5);
        assertThat(rig.fixture.result(handle.result()).status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    @Test
    void aRequestDeadlineThatPassesBeforeAnyByteWasWrittenIsNotProcessed() throws Exception {
        rig = NettyEngineRig.client(tuning());
        rig.channel.capacity(0);
        RequestHandle handle = rig.connection.request(
                Request.of(NettyEngineRig.TYPE, BODY), RequestOptions.deadlineAfter(Duration.ofMillis(100)));
        rig.awaitStalls(1);

        rig.tick(105);

        assertThat(rig.fixture.result(handle.result()).status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.NOT_PROCESSED);
    }

    @Test
    void timeThatFlowedWhileBlockedIsNotAdvancedAgainWhenTheBlockClears() throws Exception {
        rig = NettyEngineRig.client(tuning());
        rig.channel.capacity(0);
        RequestHandle handle = send();
        rig.awaitStalls(1);
        rig.tick(ACK_TIMEOUT_MS - 400);

        rig.channel.addCapacity(PLENTY);
        long stream = rig.awaitPeerRequests(1).get(0);
        rig.tick(ACK_TIMEOUT_MS - 400);

        assertThat(rig.closed()).isFalse();
        assertThat(rig.peerRequestStreams()).hasSize(1);
        rig.peerRespond(stream, 0, new byte[] {1});
        assertThat(rig.fixture.result(handle.result()).certainty()).isEqualTo(Certainty.RESPONSE);
    }

    @Test
    void thePeersMessagesAreStillReadWhileTheWriteIsBlocked() throws Exception {
        BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
        rig = NettyEngineRig.server(received::add, tuning());
        rig.channel.capacity(0);

        rig.peer.run(endpoint -> endpoint.request(NettyEngineRig.TYPE, new byte[] {1}));

        assertThat(received.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS)).isNotNull();
    }

    @Test
    void commandsFromOtherThreadsAreHandledWhileTheWriteIsBlocked() throws Exception {
        rig = NettyEngineRig.client(tuning());
        rig.channel.capacity(0);

        RequestHandle pending = send();
        rig.awaitStalls(1);
        rig.connection.close();
        rig.settleLoop();
        assertThat(pending.result().isDone()).isFalse();

        rig.connection.abort();

        assertThat(rig.fixture.result(pending.result()).status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(rig.fixture.result(rig.connection.closed()).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
    }

    @Test
    void aWriteFailureWhileBlockedEndsTheConnectionAndTheWaitingRequest() throws Exception {
        rig = NettyEngineRig.client(tuning());
        rig.channel.capacity(10);
        RequestHandle handle = send();
        rig.awaitStalls(1);

        rig.channel.failWrites();
        rig.channel.addCapacity(PLENTY);

        assertThat(rig.fixture.result(handle.result()).status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(rig.fixture.result(rig.connection.closed()).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
    }
}
