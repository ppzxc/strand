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

        rig.client.shutdownOutput();
        rig.client.addWriteCapacity(PLENTY);
        rig.settle();

        assertThat(handle.result().isDone()).isTrue();
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.UNAVAILABLE);

        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(rig.fixture.result(rig.connection.closed()).kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
    }
}
