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

/**
 * 시계(기한·확인 대기·유휴 PING)가 엔진에서 제때 돌고 입력과 순서가 맞는지 본다. nio의 {@code EngineClockTest}에 대응한다. 수동 시계와 실제 채널을 쓴다. 요청 기한의 결과 분류(쓰기
 * 전·중·후)는 {@link NettyRequestOutcomeTest}가 본다. 상대에게 닿지 않게 삼킨 쓰기({@link NettyEngineRig.TestChannel#mute})로 엔진이 낸 프레임을 직접 들여다본다.
 */
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

    /** 삼킨 프레임 가운데 PING(ACK 아님)의 수. 삼킨 것은 누적해 센다. */
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
        // 쓰기는 성공한 것처럼 돌아오지만 상대에게 닿지 않는다. 상대는 아무것도 확인하지 않는다.
        rig.channel.mute();
        RequestHandle first = send();
        RequestHandle second = send();
        rig.settleLoop();

        // 유휴 PING이 나가지만 확인이 오지 않는다.
        rig.tick(IDLE_INTERVAL_MS);
        rig.tick(ACK_TIMEOUT_MS);
        assertThat(first.result().isDone()).isFalse();

        // 확인 대기 초과로 연결이 끝나고(상대가 끊지 않으니 마무리 상한까지 기다린 뒤) 남은 요청이 모두 끝난다.
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
        // 엔진이 낸 PING이 상대에 닿지 않게 삼키고, 상대(코어 엔드포인트)가 그것을 받아 낸 확인을 따로 쥔다.
        rig.channel.mute();
        rig.tick(IDLE_INTERVAL_MS);
        byte[] ping = rig.channel.swallowedBytes();
        assertThat(ping).isNotEmpty();
        byte[] acknowledgement = rig.peer.runDetached(endpoint -> EndpointFeed.receive(endpoint, ping));
        assertThat(acknowledgement).isNotEmpty();

        // 엔진이 자는 동안 시계만 확인 대기 기한을 넘기고, 확인은 그 시계와 같은 걸음에 읽힌다(입력이 시계보다 먼저 처리된다).
        rig.inSameStep(() -> rig.clock.addAndGet(TimeUnit.SECONDS.toNanos(5)), acknowledgement);
        // 확인 대기 초과로 오인했다면 엔진은 같은 걸음에 오류 CLOSE를 쓰고 마무리 상한(100밀리초) 뒤 끝난다.
        rig.tick(200);

        // 건강하면 흐른 시계만큼 다음 유휴 PING이 하나 나갈 뿐이다.
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

    /** 기한이 있으면 엔진은 입력도 호출도 없이 스스로 깨어나 그 기한을 본다. 기한과 유휴 PING이 없으면 깨울 것이 없다(netty는 통로 대기가 아니라 타이머를 쓴다). */
    @Test
    void theEngineWakesItselfForTheNextDeadlineWithoutAnyInput() throws Exception {
        rig = NettyEngineRig.client(tuning(config -> config.idleIntervalMs(0)));
        RequestHandle handle = rig.connection.request(
                Request.of(NettyEngineRig.TYPE, BODY), RequestOptions.deadlineAfter(Duration.ofMillis(100)));
        rig.awaitPeerRequests(1);

        // 시계만 올린다. 걸음을 돌리지 않아도 기한 타이머가 엔진을 깨워 올라간 시계를 본다.
        rig.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(105));

        assertThat(rig.fixture.result(handle.result()).status()).isEqualTo(Status.DEADLINE_EXCEEDED);
    }

    @Test
    void aFloodOfInputDoesNotStarveTheClockAndTheRestIsReadOnTheFollowingSteps() throws Exception {
        rig = NettyEngineRig.client(tuning(config ->
                        config.maxFrameSize(16_384).maxConcurrentStreams(2).idleIntervalMs(0))
                .perConnection(10_000));
        // 쓰기를 막아 ACK가 쌓이게 하면 엔진이 상한에서 읽기를 멈추므로, 입력이 아직 소켓에 남은 채로 기한이 처리되는지 알 수 있다.
        rig.channel.capacity(0);
        RequestHandle handle = rig.connection.request(
                Request.of(NettyEngineRig.TYPE, BODY), RequestOptions.deadlineAfter(Duration.ofMillis(100)));
        rig.awaitStalls(1);
        AtomicLong writtenWhenEnded = new AtomicLong(-1);
        handle.result().observe(result -> writtenWhenEnded.set(rig.channel.written()));

        // 시계가 기한을 넘긴 채로 입력이 쏟아진다.
        rig.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(105));
        java.util.concurrent.CompletableFuture<Void> flood = rig.flood(FLOOD_SIZE, index -> pingFrame());

        assertThat(rig.fixture.result(handle.result()).status()).isEqualTo(Status.DEADLINE_EXCEEDED);
        // 기한이 처리된 뒤에도 상대의 입력은 소켓에 남아 있다(받기만 하다 굶지 않았다).
        NettyRig.awaitTrue(() -> !rig.channel.config().isAutoRead());
        assertThat(flood.isDone()).isFalse();

        // 남은 것도 다음 걸음들에서 이어 받는다: 쓰기가 풀리면 모두 읽고 ACK가 나간다.
        rig.channel.addCapacity(100_000_000);
        flood.get(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        NettyRig.awaitTrue(() -> rig.channel.written() >= (long) FLOOD_SIZE * PING_FRAME);
    }
}
