package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EngineReadPauseTest {
    private static final int PING_TYPE = 0x0002;
    private static final int HEADER = 20;
    private static final int MAX_FRAME_SIZE = 16_384;
    private static final long UNRESPONSIVE_ERROR_CODE = 6;
    private static final long ACK_TIMEOUT_MS = 1000;
    private static final long IDLE_INTERVAL_MS = 3000;
    private static final long FINISH_MS = 100;
    private static final int PING_FRAME = HEADER + 8;
    private static final int FLOOD_SIZE = 20_000;
    private static final long PLENTY = 100_000_000;

    private static final long STALLING_PER_CONNECTION = 8;

    private static final long STALL_SETTLE_MS = 200;
    private static final long STALL_OBSERVATION_MS = 1000;
    private static final long MAX_AWAIT_CALLS_WHILE_STALLED = 100;

    private final FloodPassage flood = new FloodPassage();
    private EngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private static UnaryOperator<EndpointConfig> config(long maxConcurrentStreams) {
        return config -> config.maxFrameSize(MAX_FRAME_SIZE)
                .maxConcurrentStreams(maxConcurrentStreams)
                .ackTimeoutMs(ACK_TIMEOUT_MS)
                .idleIntervalMs(IDLE_INTERVAL_MS);
    }

    private EngineRig.Tuning tuning(long ownStreams, long peerStreams) {
        return new EngineRig.Tuning()
                .config(config(ownStreams))
                .peerConfig(config(peerStreams))
                .passage(flood::around)
                .perConnection(10_000)
                .closeFinish(Duration.ofMillis(FINISH_MS));
    }

    private void tick(long ms) {
        rig.advance(Duration.ofMillis(ms));
        rig.settle();
    }

    private static byte[] frame(int type, int flags, int status, long streamId, byte[] payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int length = HEADER - 4 + payload.length;
        out.write(length >>> 24);
        out.write(length >>> 16);
        out.write(length >>> 8);
        out.write(length);
        out.write(type >>> 8);
        out.write(type);
        out.write(flags);
        out.write(status);
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.write((int) (streamId >>> shift));
        }
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(0);
        out.write(payload, 0, payload.length);
        return out.toByteArray();
    }

    private static byte[] ping(int index) {
        return frame(PING_TYPE, 0, 0, 0, new byte[8]);
    }

    private static byte[] requestToClient(int index) {
        return frame(EngineRig.TYPE, 0, 0, 2L * (index + 1), new byte[100]);
    }

    private static byte[] requestToServer(int index) {
        return frame(EngineRig.TYPE, 0, 0, 2L * index + 1, new byte[100]);
    }

    private void assertEndsAsUnresponsive() throws Exception {
        tick(IDLE_INTERVAL_MS);
        tick(ACK_TIMEOUT_MS);
        assertThat(rig.connection.closed().isDone()).isFalse();
        tick(1);
        tick(FINISH_MS);
        CloseNotice notice = rig.fixture.result(rig.connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
    }

    @Test
    void aPeerThatSendsOnlyPingsWithoutReadingIsReadOnlyUpToTheQueueLimitAndThenTheConnectionEnds() throws Exception {
        rig = EngineRig.client(tuning(2, 2));
        rig.client.writeCapacity(0);

        flood.flood(FLOOD_SIZE, EngineReadPauseTest::ping);
        rig.settle();

        long limitFrames = (2L * 2 * MAX_FRAME_SIZE + MAX_FRAME_SIZE) / PING_FRAME;
        assertThat(flood.delivered()).isBetween((int) limitFrames - 10, (int) limitFrames + 64 + 10);
        assertThat(flood.remaining()).isPositive();

        assertEndsAsUnresponsive();
    }

    @Test
    void aPeerThatSendsOnlyRequestsWithoutReadingEndsTheConnectionOnTheRefusalPath() throws Exception {

        rig = EngineRig.client(tuning(0, 2));
        rig.client.writeCapacity(0);

        flood.flood(FLOOD_SIZE, EngineReadPauseTest::requestToClient);
        rig.settle();

        assertThat(flood.delivered()).isBetween(1000, 2300);
        assertThat(flood.remaining()).isPositive();

        assertEndsAsUnresponsive();
    }

    @Test
    void aPeerThatSendsOnlyRequestsWithoutReadingEndsTheConnectionOnTheResponsePath() throws Exception {

        rig = EngineRig.server(
                request -> {
                    request.respond(0, new byte[100]);
                },
                tuning(2, 2));
        rig.client.writeCapacity(0);

        flood.flood(FLOOD_SIZE, EngineReadPauseTest::requestToServer);
        rig.settle();

        assertThat(flood.delivered()).isBetween(300, 5000);
        assertThat(flood.remaining()).isPositive();

        assertEndsAsUnresponsive();
    }

    @Test
    void readingStartsAgainWhenTheQueueDropsBelowTheLimit() throws Exception {
        rig = EngineRig.client(tuning(2, 2));
        rig.client.writeCapacity(0);
        flood.flood(FLOOD_SIZE, EngineReadPauseTest::ping);
        rig.settle();
        assertThat(flood.remaining()).isPositive();

        rig.client.addWriteCapacity(PLENTY);
        rig.settle();

        assertThat(flood.remaining()).isZero();
        assertThat(flood.delivered()).isEqualTo(FLOOD_SIZE);
        assertThat(rig.connection.closed().isDone()).isFalse();
    }

    @Test
    void twoCompliantPeersThatFillTheFlowControlLimitDoNotTripTheReadPauseOrDeadlock() throws Exception {
        long concurrent = 2;
        List<IncomingRequest> received = new CopyOnWriteArrayList<>();
        byte[] body = new byte[MAX_FRAME_SIZE - HEADER];
        rig = EngineRig.server(
                request -> {
                    received.add(request);
                    request.respond(0, body);
                },
                tuning(concurrent, concurrent));

        rig.client.writeCapacity(0);
        List<RequestHandle> mine = new ArrayList<>();
        for (int i = 0; i < concurrent; i++) {
            mine.add(rig.connection.request(Request.of(EngineRig.TYPE, body)));
            rig.peer.request(EngineRig.TYPE, body);
        }
        rig.settle();
        waitFor(() -> received.size() == concurrent);
        rig.settle();

        flood.flood(1, EngineReadPauseTest::ping);
        rig.settle();
        assertThat(flood.remaining()).isZero();
        assertThat(flood.delivered()).isEqualTo(1);

        rig.client.addWriteCapacity(PLENTY);
        rig.settle();
        for (long stream : rig.peerRequestStreams()) {
            rig.peerRespond(stream, 0, new byte[] {1});
        }
        assertThat(rig.connection.closed().isDone()).isFalse();
        assertThat(mine)
                .allSatisfy(handle -> assertThat(handle.result().isDone()).isTrue());
        assertThat(rig.peerRequestStreams()).hasSize((int) concurrent);
    }

    @Test
    void theAbsoluteLimitFromTheSettingReachesTheEngine() throws Exception {
        rig = EngineRig.client(tuning(2, 2).writeQueueBytes(MAX_FRAME_SIZE));
        rig.client.writeCapacity(0);

        flood.flood(FLOOD_SIZE, EngineReadPauseTest::ping);
        rig.settle();

        int capFrames = MAX_FRAME_SIZE / PING_FRAME;
        assertThat(flood.delivered()).isBetween(capFrames - 10, capFrames + 64 + 10);
    }

    @Test
    void theTransportThreadDoesNotWakeUpEmptyWhileReadingIsPaused() throws Exception {
        rig = EngineRig.client(tuning(2, 2));
        rig.client.writeCapacity(0);
        flood.flood(FLOOD_SIZE, EngineReadPauseTest::ping);
        rig.settle();
        assertThat(flood.remaining()).isPositive();

        long calls = rig.client.awaitCalls();

        rig.settle();
        assertThat(rig.client.awaitCalls()).isEqualTo(calls);
    }

    @Test
    void theTransportThreadDoesNotSpinWhileInputIsStalledOnBlockedOutput() throws Exception {
        rig = EngineRig.client(tuning(2, 2).perConnection(STALLING_PER_CONNECTION));
        rig.client.writeCapacity(0);
        flood.flood(FLOOD_SIZE, EngineReadPauseTest::ping);

        Thread.sleep(STALL_SETTLE_MS);
        assertThat(flood.remaining()).isPositive();

        long calls = rig.client.awaitCalls();
        Thread.sleep(STALL_OBSERVATION_MS);

        assertThat(rig.client.awaitCalls() - calls).isLessThanOrEqualTo(MAX_AWAIT_CALLS_WHILE_STALLED);
    }

    @Test
    void inputStalledOnBlockedOutputResumesWhenWritesAreAllowed() throws Exception {
        rig = EngineRig.client(tuning(2, 2).perConnection(STALLING_PER_CONNECTION));
        rig.client.writeCapacity(0);
        flood.flood(FLOOD_SIZE, EngineReadPauseTest::ping);
        Thread.sleep(STALL_SETTLE_MS);
        int deliveredBefore = flood.delivered();

        rig.client.writeCapacity(PLENTY);
        waitFor(() -> flood.delivered() > deliveredBefore);
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean() && System.nanoTime() < limit) {
            Thread.sleep(5);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
