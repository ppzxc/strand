package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.Event;
import io.github.ppzxc.strand.nio.internal.Inbound;
import io.github.ppzxc.strand.nio.internal.Passage;
import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 연결이 어떻게 끝나든 요청 결과와 종료 알림이 한 번만, 맞는 종류와 원인으로 확정되는지 본다. 구 연결 드라이버의 종료 알림·CLOSED 뒤 마무리·요청 끝남 시험을 제품 엔진
 * 위로 옮긴 것이다. 수동 시계와 메모리 통로를 쓰고, 상대는 코어 엔드포인트다. 종료 알림은 전송 스레드가 끝날 때 확정되므로 알림을 읽는 것이 곧 끝남을 기다리는 것이다.
 */
class EngineConnectionEndTest {
    private static final byte[] BODY = new byte[20];
    private static final long FINISH_MS = 100;

    /** 통로를 감싸 고장을 주입한다. 출력을 닫은 뒤에 읽기를 실패시키는 고장도 낼 수 있다. */
    private static final class FaultyPassage implements Passage {
        private Passage inner;
        volatile boolean failReadsAfterShutdown;
        private volatile boolean shutdownDone;
        final IOException readFailure = new IOException("읽기 실패");
        volatile boolean failReads;

        Passage around(Passage passage) {
            this.inner = passage;
            return this;
        }

        @Override
        public Inbound await(long timeoutMs, boolean waitForRead, boolean waitForWrite) throws IOException {
            if (failReads || (failReadsAfterShutdown && shutdownDone)) {
                throw readFailure;
            }
            return inner.await(timeoutMs, waitForRead, waitForWrite);
        }

        @Override
        public int writeBytes(byte[] bytes, int offset, int length) throws IOException {
            return inner.writeBytes(bytes, offset, length);
        }

        @Override
        public boolean pendingOutput() {
            return inner.pendingOutput();
        }

        @Override
        public void wakeup() {
            inner.wakeup();
        }

        @Override
        public void shutdownOutput() {
            shutdownDone = true;
            inner.shutdownOutput();
        }

        @Override
        public void close() {
            inner.close();
        }
    }

    private final FaultyPassage faulty = new FaultyPassage();
    private EngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private EngineRig.Tuning tuning() {
        return new EngineRig.Tuning().passage(faulty::around).closeFinish(Duration.ofMillis(FINISH_MS));
    }

    private RequestHandle send() {
        return rig.connection.request(Request.of(EngineRig.TYPE, BODY));
    }

    private CloseNotice notice() throws Exception {
        return rig.fixture.result(rig.connection.closed());
    }

    /** 시계를 마무리 상한 너머로 올려 가며 연결이 끝나기를 기다린다(끝나기 전에 올린 시계는 마무리 상한을 새로 시작하지 못한다). */
    private CloseNotice noticeAfterAdvancing() throws Exception {
        long limit = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!rig.connection.closed().isDone() && System.nanoTime() < limit) {
            rig.advance(Duration.ofMillis(FINISH_MS + 5));
            Thread.sleep(5);
        }
        return notice();
    }

    private void tick(long ms) {
        rig.advance(Duration.ofMillis(ms));
        rig.settle();
    }

    private static void assertOutcome(RequestHandle handle, int status, Certainty certainty) {
        assertThat(handle.result().isDone()).isTrue();
        assertThat(handle.result().resultNow().status()).isEqualTo(status);
        assertThat(handle.result().resultNow().certainty()).isEqualTo(certainty);
    }

    @Test
    void closingTheConnectionLetsPendingStreamsFinishThenBothSidesAreToldItWasAClose() throws Exception {
        rig = EngineRig.client(tuning());
        RequestHandle pending = send();
        rig.settle();

        rig.connection.close();
        rig.settle();
        assertThat(pending.result().isDone()).isFalse();

        rig.peerRespond(rig.peerRequestStreams().get(0), 0, new byte[] {7});
        assertThat(pending.result().isDone()).isTrue();
        assertThat(pending.result().resultNow().certainty()).isEqualTo(Certainty.RESPONSE);

        rig.peerHangsUp();
        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.sender()).contains(CloseNotice.Sender.LOCAL);
        assertThat(rig.peerEventKinds()).contains(Event.Kind.CLOSED);
    }

    @Test
    void aRequestIssuedWhileClosingEndsWithTheUnavailableResultNotAnException() throws Exception {
        rig = EngineRig.client(tuning());
        RequestHandle pending = send();
        rig.settle();
        rig.connection.close();
        rig.settle();

        RequestHandle late = send();
        rig.settle();

        assertOutcome(late, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
        assertThat(pending.result().isDone()).isFalse();
    }

    @Test
    void cancellingWhileClosingEndsWithTheCancelledResult() throws Exception {
        rig = EngineRig.client(tuning());
        RequestHandle pending = send();
        rig.settle();
        rig.connection.close();
        rig.settle();

        pending.cancel();
        rig.settle();

        assertOutcome(pending, Status.CANCELLED, Certainty.UNKNOWN);
    }

    @Test
    void whenThePeerDropsTheConnectionTheNoticeCarriesTheEndOfStreamException() throws Exception {
        rig = EngineRig.client(tuning());
        RequestHandle pending = send();
        rig.settle();

        rig.peerHangsUp();

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).get().isInstanceOf(EOFException.class);
        assertOutcome(pending, Status.UNAVAILABLE, Certainty.UNKNOWN);
    }

    @Test
    void whenThePeerHangsUpWhileClosingTheNoticeIsOneDisconnectWithTheEndOfStreamException() throws Exception {
        // 종료 중(CLOSE를 보냈고 스트림이 남음)에 EOF가 오면 입출력 예외가 기록된 끊김으로 한 번만 알린다.
        rig = EngineRig.client(tuning());
        RequestHandle pending = send();
        rig.settle();
        rig.connection.close();
        rig.settle();

        rig.peerHangsUp();

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).get().isInstanceOf(EOFException.class);
        assertOutcome(pending, Status.UNAVAILABLE, Certainty.UNKNOWN);
    }

    @Test
    void aReadFailureIsCarriedInTheNotice() throws Exception {
        rig = EngineRig.client(tuning());
        faulty.failReads = true;
        rig.client.wakeup();

        CloseNotice notice = notice();

        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).containsSame(faulty.readFailure);
    }

    @Test
    void aWriteFailureIsCarriedInTheNotice() throws Exception {
        rig = EngineRig.client(tuning());
        rig.client.failWrites();

        send();

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).get().isInstanceOf(IOException.class);
    }

    @Test
    void afterTheCloseExchangeTheEngineClosesOnlyItsOutputAndEndsOnceThePeerHangsUp() throws Exception {
        rig = EngineRig.client(tuning());

        rig.connection.close();
        rig.settle();

        // 보내는 쪽만 닫고 상대가 끊을 때까지 읽는다: 상대는 끝을 읽었지만 연결은 아직 끝나지 않았다.
        assertThat(rig.peerSawEnd).isTrue();
        assertThat(rig.connection.closed().isDone()).isFalse();

        rig.peerHangsUp();

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.cause()).isEmpty();
    }

    @Test
    void aReadFailureAfterClosedIsNotANewNotice() throws Exception {
        rig = EngineRig.client(tuning());
        faulty.failReadsAfterShutdown = true;

        rig.connection.close();
        rig.settle();

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.cause()).isEmpty();
    }

    @Test
    void aWriteFailureRightAfterClosedIsNeitherInTheNoticeNorANewOne() throws Exception {
        rig = EngineRig.client(tuning());
        // 엔진은 상대의 CLOSE를 받아 답하며 CLOSED를 내는 같은 차례에 쓰기가 실패한다. CLOSED가 먼저 확정된다.
        rig.client.failWrites();

        rig.peer.close(0);
        rig.pumpPeer();

        CloseNotice notice = noticeAfterAdvancing();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.sender()).contains(CloseNotice.Sender.PEER);
        assertThat(notice.cause()).isEmpty();
    }

    @Test
    void closeAndEndOfStreamReadTogetherAreOneCloseNoticeWithoutAnIoException() throws Exception {
        // 상대의 CLOSE와 EOF가 잇따라 온다. CLOSE 교환이 먼저 처리되므로 EOF는 종료 뒤의 일이다.
        rig = EngineRig.client(tuning());
        rig.peer.close(0);
        rig.pumpPeer();
        rig.peerHangsUp();

        CloseNotice notice = notice();

        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.sender()).contains(CloseNotice.Sender.PEER);
        assertThat(notice.cause()).isEmpty();
    }

    @Test
    void anInputTheEndpointRejectsEndsTheConnectionAndPendingRequestsGetTheCoreResult() throws Exception {
        rig = EngineRig.client(tuning());
        RequestHandle pending = send();
        rig.settle();

        // 길이가 머리보다 짧은 프레임은 프로토콜 위반이다.
        rig.client.inject(new byte[] {0, 0, 0, 1, 0, 0, 0, 0});

        CloseNotice notice = noticeAfterAdvancing();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.sender()).contains(CloseNotice.Sender.LOCAL);
        assertOutcome(pending, Status.UNAVAILABLE, Certainty.UNKNOWN);
    }

    @Test
    void aRequestOverThePeersConcurrentStreamLimitEndsAtOnceWithUnavailableAndNotProcessed() throws Exception {
        rig = EngineRig.client(tuning().peerConfig(config -> config.maxConcurrentStreams(1)));
        RequestHandle first = send();
        rig.settle();

        RequestHandle second = send();
        rig.settle();

        assertOutcome(second, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
        assertThat(first.result().isDone()).isFalse();
        assertThat(rig.connection.closed().isDone()).isFalse();
    }
}
