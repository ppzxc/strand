package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

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

class NettyEngineClockTest {
    private static final long ACK_TIMEOUT_MS = 1000;
    private static final long IDLE_INTERVAL_MS = 3000;
    private static final int PING_FRAME_TYPE = 0x0002;
    private static final int ACK_FLAG = 0x01;
    private static final int FRAME_HEADER_SIZE = 20;
    private static final int PING_FRAME = FRAME_HEADER_SIZE + 8;
    private static final int FLOOD_SIZE = 20_000;
    private static final byte[] BODY = new byte[20];

    private NettyEngineRig rig;
    private final ByteArrayOutputStream swallowed = new ByteArrayOutputStream();

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private static NettyEngineRig.Tuning tuning(
            UnaryOperator<io.github.ppzxc.strand.core.internal.EndpointConfig> config) {
        return new NettyEngineRig.Tuning().config(config);
    }

    private RequestHandle send() {
        return rig.connection.request(Request.of(NettyEngineRig.TYPE, BODY));
    }

    private int pingsWritten() {
        byte[] more = rig.channel.swallowedBytes();
        swallowed.write(more, 0, more.length);
        byte[] all = swallowed.toByteArray();
        int count = 0;
        for (int offset = 0; offset + FRAME_HEADER_SIZE <= all.length; ) {
            int length = ((all[offset] & 0xFF) << 24)
                    | ((all[offset + 1] & 0xFF) << 16)
                    | ((all[offset + 2] & 0xFF) << 8)
                    | (all[offset + 3] & 0xFF);
            int type = ((all[offset + 4] & 0xFF) << 8) | (all[offset + 5] & 0xFF);
            boolean ack = (all[offset + 6] & ACK_FLAG) != 0;
            if (type == PING_FRAME_TYPE && !ack) {
                count++;
            }
            offset += 4 + length;
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
        rig = NettyEngineRig.client(
                tuning(config -> config.ackTimeoutMs(ACK_TIMEOUT_MS).idleIntervalMs(IDLE_INTERVAL_MS))
                        .closeFinish(Duration.ofMillis(100)));
        rig.channel.mute();
        RequestHandle first = send();
        RequestHandle second = send();
        rig.settleLoop();

        rig.tick(IDLE_INTERVAL_MS);
        rig.tick(ACK_TIMEOUT_MS);
        assertThat(first.result().isDone()).isFalse();

        rig.tick(5);
        rig.tick(100);
        assertThat(rig.fixture.result(first.result()).status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(rig.fixture.result(second.result()).status()).isEqualTo(Status.UNAVAILABLE);
    }

    @Test
    void anAcknowledgementReadInTheSameStepAsTheElapsedTimeIsNotMistakenForATimeout() throws Exception {
        rig = NettyEngineRig.client(
                tuning(config -> config.ackTimeoutMs(ACK_TIMEOUT_MS).idleIntervalMs(IDLE_INTERVAL_MS))
                        .closeFinish(Duration.ofMillis(100)));
        rig.channel.mute();
        rig.tick(IDLE_INTERVAL_MS);
        byte[] ping = rig.channel.swallowedBytes();
        assertThat(ping).isNotEmpty();
        byte[] acknowledgement = rig.peer.runDetached(endpoint -> EndpointFeed.receive(endpoint, ping));
        assertThat(acknowledgement).isNotEmpty();

        rig.inSameStep(() -> rig.clock.addAndGet(TimeUnit.SECONDS.toNanos(5)), acknowledgement);
        rig.tick(200);

        byte[] after = rig.channel.swallowedBytes();
        assertThat(after).hasSize(PING_FRAME);
        assertThat(((after[4] & 0xFF) << 8) | (after[5] & 0xFF)).isEqualTo(PING_FRAME_TYPE);
        assertThat(rig.closed()).isFalse();
    }

    @Test
    void anIdleConnectionSendsAnIdlePing() throws Exception {
        rig = NettyEngineRig.client(tuning(config -> config.idleIntervalMs(IDLE_INTERVAL_MS)));
        rig.channel.mute();
        assertThat(pingsWritten()).isZero();

        rig.tick(IDLE_INTERVAL_MS - 1);
        assertThat(pingsWritten()).isZero();

        rig.tick(1);
        assertThat(pingsWritten()).isEqualTo(1);
    }

    @Test
    void theEngineWakesItselfForTheNextDeadlineWithoutAnyInput() throws Exception {
        rig = NettyEngineRig.client(tuning(config -> config.idleIntervalMs(0)));
        RequestHandle handle = rig.connection.request(
                Request.of(NettyEngineRig.TYPE, BODY), RequestOptions.deadlineAfter(Duration.ofMillis(100)));
        rig.awaitPeerRequests(1);

        rig.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(105));

        assertThat(rig.fixture.result(handle.result()).status()).isEqualTo(Status.DEADLINE_EXCEEDED);
    }

    @Test
    void aFloodOfInputDoesNotStarveTheClockAndTheRestIsReadOnTheFollowingSteps() throws Exception {
        rig = NettyEngineRig.client(tuning(config ->
                        config.maxFrameSize(16_384).maxConcurrentStreams(2).idleIntervalMs(0))
                .perConnection(10_000));
        rig.channel.capacity(0);
        RequestHandle handle = rig.connection.request(
                Request.of(NettyEngineRig.TYPE, BODY), RequestOptions.deadlineAfter(Duration.ofMillis(100)));
        rig.awaitStalls(1);
        AtomicLong writtenWhenEnded = new AtomicLong(-1);
        handle.result().observe(result -> writtenWhenEnded.set(rig.channel.written()));

        rig.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(105));
        java.util.concurrent.CompletableFuture<Void> flood = rig.flood(FLOOD_SIZE, index -> pingFrame());

        assertThat(rig.fixture.result(handle.result()).status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        NettyRig.awaitTrue(() -> !rig.channel.config().isAutoRead());
        assertThat(flood.isDone()).isFalse();

        rig.channel.addCapacity(100_000_000);
        flood.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        NettyRig.awaitTrue(() -> rig.channel.written() >= (long) FLOOD_SIZE * PING_FRAME);
    }
}
