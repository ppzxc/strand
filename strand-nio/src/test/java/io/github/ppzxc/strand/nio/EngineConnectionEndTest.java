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

class EngineConnectionEndTest {
    private static final byte[] BODY = new byte[20];
    private static final long FINISH_MS = 100;

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
