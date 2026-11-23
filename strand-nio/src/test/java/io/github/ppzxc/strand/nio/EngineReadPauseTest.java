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

/**
 * 쓰기 대기열 상한과 읽기 멈춤. 구 연결 드라이버의 읽기 멈춤 시험을 제품 엔진 위로 옮긴 것이다. 읽지 않고 보내기만 하는 상대 앞에서 엔진의 쓰기 대기열이
 * 상한에 묶이고(읽기를 멈추고), 시계와 명령은 계속 돌아 결국 확인 대기 초과로 끊기는지, 규칙을 지키는 상대는 읽기 멈춤에 걸리지 않는지 본다. 읽지 않는 상대는 엔진 쪽
 * 통로의 쓰기 용량 0으로 만들고, 상대가 보내는 것은 통로를 감싼 {@link FloodPassage}가 만든다. 상한 셈 자체는 core의 {@code WriteQueueLimitTest}가 본다.
 */
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

    /** 엔진이 클라이언트일 때 상대(서버) 쪽 번호 공간의 요청. 번호는 2부터 2씩 는다. */
    private static byte[] requestToClient(int index) {
        return frame(EngineRig.TYPE, 0, 0, 2L * (index + 1), new byte[100]);
    }

    /** 엔진이 서버일 때 상대(클라이언트) 쪽 번호 공간의 요청. 번호는 1부터 2씩 는다. */
    private static byte[] requestToServer(int index) {
        return frame(EngineRig.TYPE, 0, 0, 2L * index + 1, new byte[100]);
    }

    /** 확인 대기 초과로 끊김: 판정은 기한(+1ms)에 나고 마무리 상한이 차면 종료 알림이 확정된다. */
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

        // ACK PING이 28바이트씩 쓰기 대기열에 쌓인다. 상한에서 읽기를 멈추므로 끝까지 읽지 않는다.
        long limitFrames = (2L * 2 * MAX_FRAME_SIZE + MAX_FRAME_SIZE) / PING_FRAME;
        assertThat(flood.delivered()).isBetween((int) limitFrames - 10, (int) limitFrames + 64 + 10);
        assertThat(flood.remaining()).isPositive();

        // 읽기를 멈춰도 시계는 간다. 받는 것이 없으니 유휴 PING이 나가고 확인 대기가 초과된다.
        assertEndsAsUnresponsive();
    }

    @Test
    void aPeerThatSendsOnlyRequestsWithoutReadingEndsTheConnectionOnTheRefusalPath() throws Exception {
        // 내 동시 스트림이 0이라 상대 요청은 모두 RESET(REFUSED)이 쓰기 대기열에 쌓인다.
        rig = EngineRig.client(tuning(0, 2));
        rig.client.writeCapacity(0);

        flood.flood(FLOOD_SIZE, EngineReadPauseTest::requestToClient);
        rig.settle();

        // RESET(REFUSED) 한 프레임이 대기열에 쌓이는 만큼 읽고 멈춘다. 상한 49152바이트에 못 미치게 읽거나 크게 넘지 않는다.
        assertThat(flood.delivered()).isBetween(1000, 2300);
        assertThat(flood.remaining()).isPositive();

        assertEndsAsUnresponsive();
    }

    @Test
    void aPeerThatSendsOnlyRequestsWithoutReadingEndsTheConnectionOnTheResponsePath() throws Exception {
        // 핸들러가 바로 응답하면 슬롯이 비어 상대 요청이 다시 받아들여지고, 응답이 쌓인다.
        rig = EngineRig.server(
                request -> {
                    request.respond(0, new byte[100]);
                },
                tuning(2, 2));
        rig.client.writeCapacity(0);

        flood.flood(FLOOD_SIZE, EngineReadPauseTest::requestToServer);
        rig.settle();

        // 응답(120바이트)과 한도를 넘은 요청의 REFUSED(24바이트)가 섞여 쌓이므로 정확한 수는 핸들러 스레드 속도에 달렸다. 상한 안팎에서 멈춘다.
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
        // 상대가 읽지 않는 것처럼 엔진 쓰기를 막은 채, 내 요청 둘과 상대 요청 둘에 대한 응답 둘(프레임 넷, 상한은 다섯)을 대기열에 채운다.
        rig.client.writeCapacity(0);
        List<RequestHandle> mine = new ArrayList<>();
        for (int i = 0; i < concurrent; i++) {
            mine.add(rig.connection.request(Request.of(EngineRig.TYPE, body)));
            rig.peer.request(EngineRig.TYPE, body);
        }
        rig.settle();
        waitFor(() -> received.size() == concurrent);
        rig.settle();

        // 대기열이 정상 범위를 꽉 채운 채여도 읽기는 계속된다.
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
        assertThat(mine).allSatisfy(handle -> assertThat(handle.result().isDone()).isTrue());
        assertThat(rig.peerRequestStreams()).hasSize((int) concurrent);
    }

    @Test
    void theAbsoluteLimitFromTheSettingReachesTheEngine() throws Exception {
        rig = EngineRig.client(tuning(2, 2).writeQueueBytes(MAX_FRAME_SIZE));
        rig.client.writeCapacity(0);

        flood.flood(FLOOD_SIZE, EngineReadPauseTest::ping);
        rig.settle();

        // 협상 값에서 셈한 상한은 81920이지만 연결 설정의 절대 한도 16384가 먼저 걸린다.
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

        // 읽을 것이 남아 있어도 읽기를 멈춘 동안은 쓰기 가능, 깨움, 기한으로만 깨어난다.
        long calls = rig.client.awaitCalls();
        // 되풀이해 깨어난다면 통로의 활동이 가라앉지 않아 settle이 실패한다. 고정 시간을 재지 않고 그 훅으로 본다.
        rig.settle();
        assertThat(rig.client.awaitCalls()).isEqualTo(calls);
    }

    private static void waitFor(BooleanSupplier condition) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean() && System.nanoTime() < limit) {
            Thread.sleep(5);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
