package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NettyEngineReadPauseTest {
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
    private static final int SOCKET_SLACK_FRAMES = 1500;

    private NettyEngineRig rig;

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

    private static NettyEngineRig.Tuning tuning(long ownStreams, long peerStreams) {
        return new NettyEngineRig.Tuning()
                .config(config(ownStreams))
                .peerConfig(config(peerStreams))
                .perConnection(10_000)
                .closeFinish(Duration.ofMillis(FINISH_MS));
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
        return frame(NettyEngineRig.TYPE, 0, 0, 2L * (index + 1), new byte[100]);
    }

    private static byte[] requestToServer(int index) {
        return frame(NettyEngineRig.TYPE, 0, 0, 2L * index + 1, new byte[100]);
    }

    private void awaitReadPaused() throws Exception {
        NettyRig.awaitTrue(() -> !rig.channel.config().isAutoRead());
        rig.settleLoop();
        assertThat(rig.channel.config().isAutoRead()).isFalse();
    }

    private void assertEndsAsUnresponsive() throws Exception {
        rig.tick(IDLE_INTERVAL_MS);
        rig.tick(ACK_TIMEOUT_MS);
        assertThat(rig.closed()).isFalse();
        rig.tick(1);
        rig.tick(FINISH_MS);
        CloseNotice notice = rig.fixture.result(rig.connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
    }

    @Test
    void aPeerThatSendsOnlyPingsWithoutReadingIsReadOnlyUpToTheQueueLimitAndThenTheConnectionEnds() throws Exception {
        rig = NettyEngineRig.client(tuning(2, 2));
        rig.channel.capacity(0);

        CompletableFuture<Void> flood = rig.flood(FLOOD_SIZE, NettyEngineReadPauseTest::ping);
        awaitReadPaused();

        long limitFrames = (2L * 2 * MAX_FRAME_SIZE + MAX_FRAME_SIZE) / PING_FRAME;
        assertThat(flood.isDone()).isFalse();
        assertThat(rig.flooded()).isBetween((int) limitFrames - 100, (int) limitFrames + SOCKET_SLACK_FRAMES);

        assertEndsAsUnresponsive();
    }

    @Test
    void aPeerThatSendsOnlyRequestsWithoutReadingEndsTheConnectionOnTheRefusalPath() throws Exception {
        rig = NettyEngineRig.client(tuning(0, 2));
        rig.channel.capacity(0);

        CompletableFuture<Void> flood = rig.flood(FLOOD_SIZE, NettyEngineReadPauseTest::requestToClient);
        awaitReadPaused();

        assertThat(flood.isDone()).isFalse();
        assertThat(rig.flooded()).isBetween(1000, 2300 + SOCKET_SLACK_FRAMES);

        assertEndsAsUnresponsive();
    }

    @Test
    void aPeerThatSendsOnlyRequestsWithoutReadingEndsTheConnectionOnTheResponsePath() throws Exception {
        rig = NettyEngineRig.server(request -> request.respond(0, new byte[100]), tuning(2, 2));
        rig.channel.capacity(0);

        CompletableFuture<Void> flood = rig.flood(FLOOD_SIZE, NettyEngineReadPauseTest::requestToServer);
        awaitReadPaused();

        assertThat(flood.isDone()).isFalse();
        assertThat(rig.flooded()).isBetween(300, 5000 + SOCKET_SLACK_FRAMES);

        assertEndsAsUnresponsive();
    }

    @Test
    void readingStartsAgainWhenTheQueueDropsBelowTheLimit() throws Exception {
        rig = NettyEngineRig.client(tuning(2, 2));
        rig.channel.capacity(0);
        CompletableFuture<Void> flood = rig.flood(FLOOD_SIZE, NettyEngineReadPauseTest::ping);
        awaitReadPaused();
        assertThat(flood.isDone()).isFalse();

        rig.channel.addCapacity(PLENTY);

        flood.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        NettyRig.awaitTrue(() -> rig.channel.written() >= (long) FLOOD_SIZE * PING_FRAME);
        NettyRig.awaitTrue(() -> rig.channel.config().isAutoRead());
        assertThat(rig.closed()).isFalse();
    }

    @Test
    void twoCompliantPeersThatFillTheFlowControlLimitDoNotTripTheReadPauseOrDeadlock() throws Exception {
        int concurrent = 2;
        List<IncomingRequest> received = new CopyOnWriteArrayList<>();
        byte[] body = new byte[MAX_FRAME_SIZE - HEADER];
        rig = NettyEngineRig.server(
                request -> {
                    received.add(request);
                    request.respond(0, body);
                },
                tuning(concurrent, concurrent));
        rig.channel.capacity(0);
        List<RequestHandle> mine = new ArrayList<>();
        for (int i = 0; i < concurrent; i++) {
            mine.add(rig.connection.request(Request.of(NettyEngineRig.TYPE, body)));
            rig.peer.run(endpoint -> endpoint.request(NettyEngineRig.TYPE, body));
        }
        NettyRig.awaitTrue(() -> received.size() == concurrent);
        rig.settleLoop();

        assertThat(rig.channel.config().isAutoRead()).isTrue();

        rig.channel.addCapacity(PLENTY);
        for (long stream : rig.awaitPeerRequests(concurrent)) {
            rig.peerRespond(stream, 0, new byte[] {1});
        }
        for (RequestHandle handle : mine) {
            assertThat(rig.fixture.result(handle.result()).certainty()).isNotNull();
        }
        assertThat(rig.closed()).isFalse();
        assertThat(rig.peerRequestStreams()).hasSize(concurrent);
    }

    @Test
    void theAbsoluteLimitFromTheSettingReachesTheEngine() throws Exception {
        rig = NettyEngineRig.client(tuning(2, 2).writeQueueBytes(MAX_FRAME_SIZE));
        rig.channel.capacity(0);

        CompletableFuture<Void> flood = rig.flood(FLOOD_SIZE, NettyEngineReadPauseTest::ping);
        awaitReadPaused();

        int capFrames = MAX_FRAME_SIZE / PING_FRAME;
        assertThat(flood.isDone()).isFalse();
        assertThat(rig.flooded()).isBetween(capFrames - 100, capFrames + SOCKET_SLACK_FRAMES);
        assertThat(rig.flooded()).isLessThan(2925 - 100);
    }
}
