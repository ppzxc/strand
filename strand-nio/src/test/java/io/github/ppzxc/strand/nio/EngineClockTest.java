package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.Status;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EngineClockTest {
    private static final long ACK_TIMEOUT_MS = 1000;
    private static final long IDLE_INTERVAL_MS = 3000;
    private static final int PING_FRAME_TYPE = 0x0002;
    private static final int ACK_FLAG = 0x01;
    private static final int FRAME_HEADER_SIZE = 20;
    private static final int FLOOD_SIZE = 20_000;
    private static final byte[] BODY = new byte[20];

    private final FloodPassage flood = new FloodPassage();
    private EngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private static EngineRig.Tuning tuning(UnaryOperator<io.github.ppzxc.strand.core.internal.EndpointConfig> config) {
        return new EngineRig.Tuning().config(config);
    }

    private RequestHandle send() {
        return rig.connection.request(Request.of(EngineRig.TYPE, BODY));
    }

    private void tick(long ms) {
        rig.advance(Duration.ofMillis(ms));
        rig.settle();
    }

    private int pingsWritten() {
        int count = 0;
        for (byte[] frame : rig.client.written()) {
            int type = ((frame[4] & 0xFF) << 8) | (frame[5] & 0xFF);
            boolean ack = (frame[6] & ACK_FLAG) != 0;
            if (type == PING_FRAME_TYPE && !ack) {
                count++;
            }
        }
        return count;
    }

    private static byte[] pingFrame() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int length = FRAME_HEADER_SIZE - 4 + 8;
        out.write(length >>> 24);
        out.write(length >>> 16);
        out.write(length >>> 8);
        out.write(length);
        out.write(PING_FRAME_TYPE >>> 8);
        out.write(PING_FRAME_TYPE);
        out.write(0);
        out.write(0);
        for (int i = 0; i < 12; i++) {
            out.write(0);
        }
        byte[] payload = new byte[8];
        out.write(payload, 0, payload.length);
        return out.toByteArray();
    }

    @Test
    void whenThePeerNeverAcknowledgesTheConnectionEndsAndEveryPendingRequestEnds() throws Exception {
        rig = EngineRig.client(
                tuning(config -> config.ackTimeoutMs(ACK_TIMEOUT_MS).idleIntervalMs(IDLE_INTERVAL_MS))
                        .closeFinish(Duration.ofMillis(100)));

        rig.client.mute();
        RequestHandle first = send();
        RequestHandle second = send();
        rig.settle();

        tick(IDLE_INTERVAL_MS);
        tick(ACK_TIMEOUT_MS);
        assertThat(first.result().isDone()).isFalse();

        tick(5);
        tick(100);
        assertThat(first.result().isDone()).isTrue();
        assertThat(second.result().isDone()).isTrue();
        assertThat(first.result().resultNow().status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(second.result().resultNow().status()).isEqualTo(Status.UNAVAILABLE);
    }

    @Test
    void anAcknowledgementReadInTheSameStepAsTheElapsedTimeIsNotMistakenForATimeout() throws Exception {
        rig = EngineRig.client(tuning(config -> config.ackTimeoutMs(ACK_TIMEOUT_MS)));

        rig.client.holdDeliveries();
        RequestHandle handle = send();
        rig.settle();

        rig.clock.addAndGet(TimeUnit.SECONDS.toNanos(5));

        rig.client.releaseDeliveries();
        rig.settle();

        assertThat(rig.connection.closed().isDone()).isFalse();
        rig.peerRespond(rig.peerRequestStreams().get(0), 0, new byte[] {1});
        assertThat(handle.result().isDone()).isTrue();
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.RESPONSE);
    }

    @Test
    void anIdleConnectionSendsAnIdlePing() throws Exception {
        rig = EngineRig.client(tuning(config -> config.idleIntervalMs(IDLE_INTERVAL_MS)));
        assertThat(pingsWritten()).isZero();

        tick(IDLE_INTERVAL_MS - 1);
        assertThat(pingsWritten()).isZero();

        tick(1);
        assertThat(pingsWritten()).isEqualTo(1);
    }

    @Test
    void theTransportThreadWaitsWithoutALimitWhenNothingHasADeadlineAndOnlyUntilTheNextDeadlineOtherwise()
            throws Exception {
        rig = EngineRig.client(tuning(config -> config.idleIntervalMs(0)));
        rig.settle();
        assertThat(rig.client.lastAwaitTimeoutMs()).isNegative();

        long awaits = rig.client.awaitCalls();
        rig.settle();
        assertThat(rig.client.awaitCalls()).isEqualTo(awaits);

        RequestHandle handle = rig.connection.request(
                Request.of(EngineRig.TYPE, BODY), RequestOptions.deadlineAfter(Duration.ofMillis(100)));
        rig.settle();
        assertThat(rig.client.lastAwaitTimeoutMs()).isBetween(100L, 110L);

        tick(105);
        assertThat(handle.result().isDone()).isTrue();
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.DEADLINE_EXCEEDED);
    }

    @Test
    void aFloodOfInputDoesNotStarveTheClockAndTheRestIsReadOnTheFollowingSteps() throws Exception {
        rig = EngineRig.client(tuning(config -> config.idleIntervalMs(0)).passage(flood::around));
        RequestHandle handle = rig.connection.request(
                Request.of(EngineRig.TYPE, BODY), RequestOptions.deadlineAfter(Duration.ofMillis(100)));
        rig.settle();
        AtomicLong leftWhenEnded = new AtomicLong(-1);
        handle.result().observe(result -> leftWhenEnded.set(flood.remaining()));

        rig.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(105));
        flood.flood(FLOOD_SIZE, index -> pingFrame());

        waitFor(() -> handle.result().isDone());
        assertThat(handle.result().resultNow().status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        waitFor(() -> leftWhenEnded.get() >= 0);

        assertThat(leftWhenEnded.get()).isPositive();

        waitFor(() -> flood.remaining() == 0);
    }

    private static void waitFor(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean() && System.nanoTime() < limit) {
            Thread.sleep(2);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
