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

/**
 * 쓰기 대기열 상한과 읽기 멈춤. nio의 {@code EngineReadPauseTest}에 대응한다. 읽지 않고 보내기만 하는 상대 앞에서 엔진의 쓰기 대기열이 상한에 묶이고(읽기를 멈추고), 시계와
 * 명령은 계속 돌아 결국 확인 대기 초과로 끊기는지, 규칙을 지키는 상대는 읽기 멈춤에 걸리지 않는지 본다. 읽지 않는 상대는 엔진 쪽 채널의 쓰기 용량 0으로 만들고, 상대가 보내는 것은 실제
 * 소켓으로 밀어 넣는 프레임이다. netty는 읽기를 채널의 {@code autoRead}로 멈추므로, 읽기가 멈췄는지는 그 설정으로, 상한까지만 읽었는지는 엔진이 쥔 제어 출력 사용량으로 본다.
 * 상한 셈 자체는 core의 {@code WriteQueueLimitTest}가 본다.
 */
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
    /** 엔진이 읽기를 멈춘 뒤에도 소켓 버퍼(보내는 쪽과 받는 쪽, 작게 줄였다)에 들어가는 프레임 수의 넉넉한 어림. */
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

    /** 엔진이 클라이언트일 때 상대(서버) 쪽 번호 공간의 요청. 번호는 2부터 2씩 는다. */
    private static byte[] requestToClient(int index) {
        return frame(NettyEngineRig.TYPE, 0, 0, 2L * (index + 1), new byte[100]);
    }

    /** 엔진이 서버일 때 상대(클라이언트) 쪽 번호 공간의 요청. 번호는 1부터 2씩 는다. */
    private static byte[] requestToServer(int index) {
        return frame(NettyEngineRig.TYPE, 0, 0, 2L * index + 1, new byte[100]);
    }

    /** 엔진이 읽기를 멈출 때까지 기다린 뒤, 멈춘 채 가라앉았음을 짧게 두 번 본다. */
    private void awaitReadPaused() throws Exception {
        NettyRig.awaitTrue(() -> !rig.channel.config().isAutoRead());
        rig.settleLoop();
        assertThat(rig.channel.config().isAutoRead()).isFalse();
    }

    /** 확인 대기 초과로 끊김: 판정은 기한(+1ms)에 나고 마무리 상한이 차면 종료 알림이 확정된다. */
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

        // ACK PING이 28바이트씩 쓰기 대기열에 쌓인다. 상한에서 읽기를 멈추므로 끝까지 읽지 않아 상대의 나머지는 소켓에 걸려 있다.
        long limitFrames = (2L * 2 * MAX_FRAME_SIZE + MAX_FRAME_SIZE) / PING_FRAME;
        assertThat(flood.isDone()).isFalse();
        assertThat(rig.flooded()).isBetween((int) limitFrames - 100, (int) limitFrames + SOCKET_SLACK_FRAMES);

        // 읽기를 멈춰도 시계는 간다. 받는 것이 없으니 유휴 PING이 나가고 확인 대기가 초과된다.
        assertEndsAsUnresponsive();
    }

    @Test
    void aPeerThatSendsOnlyRequestsWithoutReadingEndsTheConnectionOnTheRefusalPath() throws Exception {
        // 내 동시 스트림이 0이라 상대 요청은 모두 RESET(REFUSED)이 쓰기 대기열에 쌓인다.
        rig = NettyEngineRig.client(tuning(0, 2));
        rig.channel.capacity(0);

        CompletableFuture<Void> flood = rig.flood(FLOOD_SIZE, NettyEngineReadPauseTest::requestToClient);
        awaitReadPaused();

        // RESET(REFUSED) 한 프레임이 대기열에 쌓이는 만큼 읽고 멈춘다. 상한 49152바이트에 못 미치게 읽거나 크게 넘지 않는다.
        assertThat(flood.isDone()).isFalse();
        assertThat(rig.flooded()).isBetween(1000, 2300 + SOCKET_SLACK_FRAMES);

        assertEndsAsUnresponsive();
    }

    @Test
    void aPeerThatSendsOnlyRequestsWithoutReadingEndsTheConnectionOnTheResponsePath() throws Exception {
        // 핸들러가 바로 응답하면 슬롯이 비어 상대 요청이 다시 받아들여지고, 응답이 쌓인다.
        rig = NettyEngineRig.server(request -> request.respond(0, new byte[100]), tuning(2, 2));
        rig.channel.capacity(0);

        CompletableFuture<Void> flood = rig.flood(FLOOD_SIZE, NettyEngineReadPauseTest::requestToServer);
        awaitReadPaused();

        // 응답(120바이트)과 한도를 넘은 요청의 REFUSED(24바이트)가 섞여 쌓이므로 정확한 수는 핸들러 스레드 속도에 달렸다. 상한 안팎에서 멈춘다.
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
        // 상대가 보낸 PING 전부에 ACK가 나갔다: 모두 읽었다.
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
        // 상대가 읽지 않는 것처럼 엔진 쓰기를 막은 채, 내 요청 둘과 상대 요청 둘에 대한 응답 둘(프레임 넷, 상한은 다섯)을 대기열에 채운다.
        rig.channel.capacity(0);
        List<RequestHandle> mine = new ArrayList<>();
        for (int i = 0; i < concurrent; i++) {
            mine.add(rig.connection.request(Request.of(NettyEngineRig.TYPE, body)));
            rig.peer.run(endpoint -> endpoint.request(NettyEngineRig.TYPE, body));
        }
        NettyRig.awaitTrue(() -> received.size() == concurrent);
        rig.settleLoop();

        // 대기열이 정상 범위를 꽉 채운 채여도 읽기는 계속된다.
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

        // 협상 값에서 셈한 상한은 81920이지만 연결 설정의 절대 한도 16384가 먼저 걸려, 기본 상한(2925프레임)보다 훨씬 일찍 멈춘다.
        int capFrames = MAX_FRAME_SIZE / PING_FRAME;
        assertThat(flood.isDone()).isFalse();
        assertThat(rig.flooded()).isBetween(capFrames - 100, capFrames + SOCKET_SLACK_FRAMES);
        assertThat(rig.flooded()).isLessThan(2925 - 100);
    }
}
