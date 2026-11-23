package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.nio.internal.InMemoryConnectionPair.Side;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 통로 쓰기가 막혀도(상대가 읽지 않아 쓰기 용량이 0) 전송 스레드가 읽기, 시계, 명령 처리를 계속 도는지 본다. 연결 드라이버에는 단조 시계를 주입하고, 통로는
 * 시간 제한이 실제 시간으로 흐르지 않고 쓰기 용량을 시험이 정하는 {@link InMemoryPassage}다. 쓰기 막힘은 이 용량을 0으로 두어 만든다.
 */
class ConnectionDriverBlockedWriteTest {

    private static final int APPLICATION_TYPE = 256;
    private static final int STATUS_DEADLINE_EXCEEDED = 4;
    private static final int STATUS_CANCELLED = 1;
    private static final int STATUS_UNAVAILABLE = 14;
    private static final long UNRESPONSIVE_ERROR_CODE = 6;
    private static final long ACK_TIMEOUT_MS = 1000;
    private static final long IDLE_INTERVAL_MS = 3000;
    private static final long PLENTY = 1_000_000;

    private final List<ConnectionDriver> started = new ArrayList<>();
    private final ExecutorService handlers = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "test-handler");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void stopAll() {
        started.forEach(ConnectionDriver::close);
        handlers.shutdownNow();
    }

    private static EndpointConfig config() {
        return EndpointConfig.defaults().ackTimeoutMs(ACK_TIMEOUT_MS).idleIntervalMs(IDLE_INTERVAL_MS);
    }

    /** 핸드셰이크를 마친 연결 한 쌍. */
    private Side[] connected() {
        Side[] sides = InMemoryConnectionPair.connected(config(), handlers, true);
        started.add(sides[0].driver);
        started.add(sides[1].driver);
        return sides;
    }

    /** 전송 스레드들이 가라앉고, 핸들러 실행기(하나의 스레드)에 넘어간 알림과 요청이 모두 처리되기를 기다린다. */
    private void settle(Side... sides) {
        InMemoryConnectionPair.settle(handlers, sides);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void aPeerThatDoesNotReadEndsTheConnectionWhenTheAckWaitRunsOut() {
        Side[] sides = connected();
        Side client = sides[0];
        Side server = sides[1];
        // 요청의 앞 10바이트만 나가고 막힌다. 쓰기를 시작한 요청이다.
        client.passage.writeCapacity(10);
        RequestHandle handle = client.driver.request(APPLICATION_TYPE, bytes("unread"));
        settle(client, server);

        // 유휴 PING을 보내기로 했지만 막힌 쓰기 뒤라 넘기지 못한다. 보내기로 한 때부터 확인 대기 시간이 지나면 확인 대기 초과와 같게 닫는다.
        client.tick(IDLE_INTERVAL_MS);
        settle(client, server);
        client.tick(ACK_TIMEOUT_MS);
        settle(client, server);
        assertThat(client.closed).isEmpty();

        client.tick(1);
        settle(client, server);
        assertThat(client.closed).hasSize(1);
        CloseReason reason = client.closed.peek().reason();
        assertThat(reason.kind()).isEqualTo(CloseReason.Kind.ERROR_CLOSE);
        assertThat(reason.errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
        assertThat(handle.result()).isDone();
        // 쓰기를 시작했지만 다 쓰지 못한 요청이라 결과 모름이다.
        assertThat(handle.result().join().status()).isEqualTo(STATUS_UNAVAILABLE);
        assertThat(handle.result().join().certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    @Test
    void aRequestDeadlineStillPassesWhileTheWriteIsBlocked() {
        Side[] sides = connected();
        Side client = sides[0];
        Side server = sides[1];
        // 요청의 앞 10바이트만 나가고 막힌다. 쓰기를 시작한 요청이다.
        client.passage.writeCapacity(10);
        RequestHandle handle = client.driver.request(APPLICATION_TYPE, bytes("slow"), 100);
        settle(client, server);

        client.tick(100);
        settle(client, server);
        assertThat(handle.result()).isNotDone();

        client.tick(1);
        settle(client, server);
        assertThat(handle.result()).isDone();
        assertThat(handle.result().join().status()).isEqualTo(STATUS_DEADLINE_EXCEEDED);
        assertThat(handle.result().join().certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    @Test
    void aRequestDeadlineThatPassesBeforeAnyByteWasWrittenIsNotProcessed() {
        Side[] sides = connected();
        Side client = sides[0];
        Side server = sides[1];
        // 한 바이트도 나가지 못한다. 쓰기를 시작하지 않은 요청이다.
        client.blockWrites();
        RequestHandle handle = client.driver.request(APPLICATION_TYPE, bytes("slow"), 100);
        settle(client, server);

        client.tick(101);
        settle(client, server);

        assertThat(handle.result().join().status()).isEqualTo(STATUS_DEADLINE_EXCEEDED);
        assertThat(handle.result().join().certainty()).isEqualTo(Certainty.NOT_PROCESSED);
    }

    @Test
    void theIdlePingIsDueOnScheduleWhileTheWriteIsBlocked() {
        Side[] sides = connected();
        Side client = sides[0];
        Side server = sides[1];
        client.blockWrites();

        // 유휴 PING이 제때 나가야(꺼내져 확인 대기가 돌아야) 그 확인 대기가 PING 시점부터 센다.
        client.tick(IDLE_INTERVAL_MS);
        settle(client, server);
        client.tick(ACK_TIMEOUT_MS);
        settle(client, server);
        assertThat(client.closed).isEmpty();

        client.tick(1);
        settle(client, server);
        assertThat(client.closed).hasSize(1);
        assertThat(client.closed.peek().reason().errorCode()).isEqualTo(UNRESPONSIVE_ERROR_CODE);
    }

    @Test
    void timeThatFlowedWhileBlockedIsNotAdvancedAgainWhenTheBlockClears() throws Exception {
        Side[] sides = connected();
        Side client = sides[0];
        Side server = sides[1];
        client.blockWrites();
        RequestHandle handle = client.driver.request(APPLICATION_TYPE, bytes("slow"));
        settle(client, server);
        client.tick(ACK_TIMEOUT_MS - 400);
        settle(client, server);

        client.unblockWrites(PLENTY);
        settle(client, server);
        // 서버가 요청을 받아 ACK를 돌려줬다. 풀린 뒤 시간이 다시 몰려 흐르지 않으니 연결이 그대로다.
        client.tick(ACK_TIMEOUT_MS - 400);
        settle(client, server);

        assertThat(client.closed).isEmpty();
        assertThat(server.received).hasSize(1);
        server.received.get(0).respond(0, bytes("late")).get(5, TimeUnit.SECONDS);
        settle(client, server);
        assertThat(handle.result().get(5, TimeUnit.SECONDS).certainty()).isEqualTo(Certainty.RESPONSE);
    }

    @Test
    void thePeersMessagesAreStillReadWhileTheWriteIsBlocked() throws Exception {
        Side[] sides = connected();
        Side client = sides[0];
        Side server = sides[1];
        client.blockWrites();
        long before = client.driver.receivedBytes();

        server.driver.ping(new byte[8]).get(5, TimeUnit.SECONDS);
        settle(client, server);

        assertThat(client.driver.receivedBytes()).isGreaterThan(before);
        RequestHandle fromServer = server.driver.request(APPLICATION_TYPE + 1, bytes("to-client"));
        settle(client, server);
        assertThat(client.received).hasSize(1);
        assertThat(fromServer.result()).isNotDone();
    }

    @Test
    void commandsFromOtherThreadsAreHandledWhileTheWriteIsBlocked() throws Exception {
        Side[] sides = connected();
        Side client = sides[0];
        Side server = sides[1];
        server.driver.request(APPLICATION_TYPE + 1, bytes("to-client"));
        settle(client, server);
        client.blockWrites();

        // request, respond, ping은 명령이 처리되면 끝난다.
        RequestHandle pending = client.driver.request(APPLICATION_TYPE, bytes("held"));
        client.driver.respond(client.received.get(0).streamId(), 0, bytes("re")).get(5, TimeUnit.SECONDS);
        client.driver.ping(new byte[8]).get(5, TimeUnit.SECONDS);
        settle(client, server);
        assertThat(pending.result()).isNotDone();

        // closeConnection으로 종료 중이 된 연결을 abortClosing으로 그만둔다. 둘 다 막힌 쓰기와 상관없이 처리된다.
        client.driver.closeConnection();
        settle(client, server);
        assertThat(pending.result()).isNotDone();
        client.driver.abortClosing();
        settle(client, server);
        assertThat(pending.result()).isDone();
    }

    @Test
    void writtenCountsGrowOnlyWhenAMessageWasWrittenToTheEnd() {
        Side[] sides = connected();
        Side client = sides[0];
        Side server = sides[1];
        long bytesBefore = client.driver.writtenBytes();
        long framesBefore = client.driver.writtenFrames();
        client.passage.writeCapacity(10);

        client.driver.ping(new byte[8]).join();
        settle(client, server);

        // PING 프레임은 28바이트라 10바이트만 받은 쓰기는 아직 끝나지 않았다.
        assertThat(client.driver.writtenBytes()).isEqualTo(bytesBefore);
        assertThat(client.driver.writtenFrames()).isEqualTo(framesBefore);

        client.passage.addWriteCapacity(PLENTY);
        settle(client, server);

        assertThat(client.driver.writtenBytes()).isEqualTo(bytesBefore + 28);
        assertThat(client.driver.writtenFrames()).isEqualTo(framesBefore + 1);
    }

    @Test
    void writeBlockedSignalArrivesWhenAMessageCannotBeWrittenToTheEnd() throws Exception {
        Side[] sides = connected();
        Side client = sides[0];
        client.passage.writeCapacity(10);

        client.driver.ping(new byte[8]).join();

        // PING 프레임은 28바이트라 10바이트만 받으면 나머지는 쓰기 가능을 기다린다.
        assertThat(client.driver.awaitWriteBlocked(true, 5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void cancellationAfterPartialRequestWriteFinishesFrameThenSendsReset() throws Exception {
        PartialWriteObservation.Result result =
                PartialWriteObservation.cancelAfterPartialWrite(APPLICATION_TYPE, bytes("partial"), 10);

        assertThat(result.status()).isEqualTo(STATUS_CANCELLED);
        assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
        assertThat(result.frames()).hasSize(2);
        assertThat(frameType(result.frames().get(0))).isEqualTo(APPLICATION_TYPE);
        assertThat(frameType(result.frames().get(1))).isEqualTo(4);
    }

    private static int frameType(byte[] frame) {
        return ((frame[4] & 0xff) << 8) | (frame[5] & 0xff);
    }

    @Test
    void writeBlockedSignalClearsOnceTheQueuedMessagesAreWritten() throws Exception {
        Side[] sides = connected();
        Side client = sides[0];
        Side server = sides[1];
        client.passage.writeCapacity(10);
        client.driver.ping(new byte[8]).join();
        assertThat(client.driver.awaitWriteBlocked(true, 5, TimeUnit.SECONDS)).isTrue();

        client.passage.addWriteCapacity(PLENTY);
        settle(client, server);

        assertThat(client.driver.awaitWriteBlocked(false, 5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void waitingForWriteBlockedGivesUpAfterTheTimeoutWhenNoBlockHappens() throws Exception {
        Side[] sides = connected();

        assertThat(sides[0].driver.awaitWriteBlocked(true, 20, TimeUnit.MILLISECONDS))
                .isFalse();
    }

    @Test
    void writeBlockedSignalClearsWhenTheBlockedWriteFailsWithAnIoError() throws Exception {
        Side[] sides = connected();
        Side client = sides[0];
        client.passage.writeCapacity(10);
        client.driver.ping(new byte[8]).join();
        assertThat(client.driver.awaitWriteBlocked(true, 5, TimeUnit.SECONDS)).isTrue();

        // 출력을 닫은 통로에 쓰기 가능이 오면 다음 쓰기가 입출력 오류로 끝난다.
        client.passage.shutdownOutput();
        client.passage.addWriteCapacity(PLENTY);

        assertThat(client.driver.awaitWriteBlocked(false, 5, TimeUnit.SECONDS)).isTrue();
        settle(client, sides[1]);
        assertThat(client.closed).isNotEmpty();
    }
}
