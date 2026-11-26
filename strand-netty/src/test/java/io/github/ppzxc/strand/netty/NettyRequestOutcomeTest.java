package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.ErrorCode;
import io.github.ppzxc.strand.core.internal.Event;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NettyRequestOutcomeTest {
    private static final Duration DEADLINE = Duration.ofMillis(10);
    private static final byte[] BODY = new byte[200];

    private NettyRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private RequestHandle send(Duration deadline) {
        return rig.connection.request(Request.of(NettyRig.TYPE, BODY), RequestOptions.deadlineAfter(deadline));
    }

    private RequestHandle send() {
        return rig.connection.request(Request.of(NettyRig.TYPE, BODY));
    }

    private static RequestResult assertOutcome(RequestHandle handle, int status, Certainty certainty) throws Exception {
        NettyRig.awaitTrue(() -> handle.result().isDone());
        RequestResult result = handle.result().resultNow();
        assertThat(result.status()).isEqualTo(status);
        assertThat(result.certainty()).isEqualTo(certainty);
        return result;
    }

    private long awaitPeerStream(int count) throws Exception {
        NettyRig.awaitTrue(() -> rig.peerRequestStreams().size() >= count);
        return rig.peerRequestStreams().get(count - 1);
    }

    private RequestHandle sendWithWriteLimit(long bytes, Duration deadline) throws Exception {
        long writtenBefore = rig.written();
        long stallsBefore = rig.stalls();
        rig.writeCapacity(bytes);
        RequestHandle handle = deadline == null ? send() : send(deadline);
        NettyRig.awaitTrue(() -> rig.stalls() > stallsBefore);
        assertThat(rig.written() - writtenBefore).isEqualTo(Math.min(bytes, frameBytes()));
        return handle;
    }

    private static long frameBytes() {
        return 20L + BODY.length;
    }

    private void releaseWrites() {
        rig.writeCapacity(Long.MAX_VALUE);
    }

    private void endConnection() throws Exception {
        rig.peer.disconnect();
        rig.fixture.result(rig.connection.closed());
    }

    @Test
    void aDeadlineOfZeroEndsAtOnceAsDeadlineExceededNotProcessedAndWritesNothing() throws Exception {
        rig = NettyRig.client();
        long before = rig.written();

        RequestHandle handle = send(Duration.ZERO);

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED);
        assertThat(rig.written()).isEqualTo(before);
        rig.settle();
        assertThat(rig.peerRequestStreams()).isEmpty();
        assertThat(rig.peerResets()).isEmpty();
        NettyRig.awaitTrue(() -> rig.used(Resource.OUTGOING_REQUESTS) == 0);
    }

    @Test
    void aRequestWithoutADeadlineNeverExpiresNoMatterHowFarTheClockGoes() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send();
        awaitPeerStream(1);

        rig.advance(Duration.ofHours(10));
        rig.settle();

        assertThat(handle.result().isDone()).isFalse();
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void theDeadlineCountsWhileTheCallWaitsForTheTransportBeforeCoreRegistration() throws Exception {
        rig = NettyRig.client();
        rig.closeGate();
        long before = rig.written();

        RequestHandle handle = send(DEADLINE);
        rig.advance(DEADLINE.plusMillis(1));
        rig.openGate();

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED);
        assertThat(rig.written()).isEqualTo(before);
        rig.settle();
        assertThat(rig.peerRequestStreams()).isEmpty();
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void aRequestWhoseDeadlineHoldsWhenItIsRegisteredIsSentNormally() throws Exception {
        rig = NettyRig.client();
        rig.closeGate();

        RequestHandle handle = send(DEADLINE);
        rig.advance(DEADLINE.minusMillis(5));
        rig.openGate();

        awaitPeerStream(1);
        assertThat(handle.result().isDone()).isFalse();
    }

    @Test
    void aResponseReadInTheSameStepBeatsACancelQueuedForThatStep() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send();
        long stream = awaitPeerStream(1);
        byte[] response = rig.detachedResponse(stream, Status.OK, new byte[] {7});

        rig.inSameStep(handle::cancel, response);

        RequestResult result = assertOutcome(handle, Status.OK, Certainty.RESPONSE);
        assertThat(result.payload()).containsExactly(7);
        rig.settle();
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void aResponseReadInTheSameStepBeatsAnExpiryOfTheSameStep() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send(DEADLINE);
        long stream = awaitPeerStream(1);
        byte[] response = rig.detachedResponse(stream, Status.OK, BODY);

        rig.inSameStep(() -> rig.advance(DEADLINE.plusMillis(5)), response);

        assertOutcome(handle, Status.OK, Certainty.RESPONSE);
        rig.settle();
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void aCancelQueuedWithAnExpiryOfTheSameStepIsDecidedByTheClockFirstAndKeepsASingleResult() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send(DEADLINE);
        awaitPeerStream(1);
        rig.closeGate();

        handle.cancel();
        rig.advance(DEADLINE.plusMillis(5));
        rig.openGate();

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
        NettyRig.awaitTrue(() -> rig.peerResets().size() == 1);
        rig.settle();
        assertThat(rig.peerResets()).hasSize(1);
    }

    @Test
    void aLateResponseAfterACancelDoesNotChangeTheResult() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send();
        long stream = awaitPeerStream(1);
        byte[] late = rig.detachedResponse(stream, Status.OK, BODY);

        handle.cancel();
        assertOutcome(handle, Status.CANCELLED, Certainty.UNKNOWN);
        rig.inSameStep(() -> {}, late);

        assertOutcome(handle, Status.CANCELLED, Certainty.UNKNOWN);
    }

    @Test
    void aLateResponseAfterAnExpiryDoesNotChangeTheResult() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send(DEADLINE);
        long stream = awaitPeerStream(1);
        byte[] late = rig.detachedResponse(stream, Status.OK, BODY);

        rig.advance(DEADLINE.plusMillis(5));
        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
        rig.inSameStep(() -> {}, late);

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
    }

    @Test
    void cancelBeforeAnyByteOfTheFrameWasWrittenIsCancelledNotProcessedWithoutReset() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = sendWithWriteLimit(0, null);

        handle.cancel();
        assertOutcome(handle, Status.CANCELLED, Certainty.NOT_PROCESSED);
        releaseWrites();
        rig.settle();

        assertThat(rig.peerRequestStreams()).isEmpty();
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void cancelAfterPartialProgressFinishesTheFrameThenResetsAndIsCancelledUnknown() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = sendWithWriteLimit(50, null);

        handle.cancel();
        assertOutcome(handle, Status.CANCELLED, Certainty.UNKNOWN);
        releaseWrites();
        NettyRig.awaitTrue(() -> rig.peerResets().size() == 1);

        assertThat(rig.peerRequestStreams()).hasSize(1);
        assertThat(rig.peerResets().get(0)[0])
                .isEqualTo(rig.peerRequestStreams().get(0));
        assertThat(rig.peerEventKinds()
                        .subList(
                                rig.peerEventKinds().size() - 2,
                                rig.peerEventKinds().size()))
                .containsExactly(Event.Kind.REQUEST_RECEIVED, Event.Kind.REQUEST_RESET);
    }

    @Test
    void cancelAfterTheWholeFrameWasWrittenSendsResetAndIsCancelledUnknown() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send();
        awaitPeerStream(1);

        handle.cancel();

        assertOutcome(handle, Status.CANCELLED, Certainty.UNKNOWN);
        NettyRig.awaitTrue(() -> rig.peerResets().size() == 1);
    }

    @Test
    void expiryBeforeAnyByteOfTheFrameWasWrittenIsDeadlineExceededNotProcessedWithoutReset() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = sendWithWriteLimit(0, DEADLINE);

        rig.advance(DEADLINE.plusMillis(5));
        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED);
        releaseWrites();
        rig.settle();

        assertThat(rig.peerRequestStreams()).isEmpty();
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void expiryAfterPartialProgressFinishesTheFrameThenResetsAndIsDeadlineExceededUnknown() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = sendWithWriteLimit(50, DEADLINE);

        rig.advance(DEADLINE.plusMillis(5));
        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
        releaseWrites();
        NettyRig.awaitTrue(() -> rig.peerResets().size() == 1);

        assertThat(rig.peerRequestStreams()).hasSize(1);
    }

    @Test
    void expiryAfterTheWholeFrameWasWrittenSendsResetAndIsDeadlineExceededUnknown() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send(DEADLINE);
        long stream = awaitPeerStream(1);

        rig.advance(DEADLINE.plusMillis(5));

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
        NettyRig.awaitTrue(() -> rig.peerResets().size() == 1);
        assertThat(rig.peerResets().get(0)).containsExactly(stream, ErrorCode.CANCEL);
    }

    @Test
    void aDisconnectBeforeAnyByteOfTheFrameWasWrittenIsUnavailableNotProcessed() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = sendWithWriteLimit(0, null);

        endConnection();

        assertOutcome(handle, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }

    @Test
    void aDisconnectBeforeTheLastByteOfTheFrameWasWrittenIsUnavailableNotProcessed() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = sendWithWriteLimit(50, null);

        endConnection();

        assertOutcome(handle, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }

    @Test
    void aDisconnectAfterTheLastByteOfTheFrameWasWrittenIsUnavailableUnknown() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send();
        awaitPeerStream(1);

        endConnection();

        assertOutcome(handle, Status.UNAVAILABLE, Certainty.UNKNOWN);
    }

    @Test
    void aWriteFailureAfterTheWholeFrameWasWrittenIsUnavailableUnknown() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send();
        awaitPeerStream(1);

        rig.failWrites();
        RequestHandle next = send();

        assertOutcome(handle, Status.UNAVAILABLE, Certainty.UNKNOWN);
        assertOutcome(next, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }

    @Test
    void aWriteFailureAfterAResponseWasPreservedDoesNotChangeThatResult() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send();
        rig.peerRespond(awaitPeerStream(1), Status.OK, new byte[] {1});
        assertOutcome(handle, Status.OK, Certainty.RESPONSE);

        rig.failWrites();
        send();
        rig.fixture.result(rig.connection.closed());

        assertOutcome(handle, Status.OK, Certainty.RESPONSE);
    }

    @Test
    void aDisconnectAfterAnExpiryDoesNotChangeThatResult() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send(DEADLINE);
        awaitPeerStream(1);
        rig.advance(DEADLINE.plusMillis(5));
        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);

        endConnection();

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
    }

    @Test
    void aRequestOnAnEndedConnectionIsUnavailableNotProcessedAndCancelDoesNotMakeItCancelled() throws Exception {
        rig = NettyRig.client();
        endConnection();

        RequestHandle handle = send();
        assertOutcome(handle, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
        handle.cancel();

        assertOutcome(handle, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }

    @Test
    void cancelOfAnAnsweredRequestKeepsTheResponseAsTheRetainedResult() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send();
        rig.peerRespond(awaitPeerStream(1), Status.OK, new byte[] {9});
        assertOutcome(handle, Status.OK, Certainty.RESPONSE);

        handle.cancel();
        rig.settle();

        assertOutcome(handle, Status.OK, Certainty.RESPONSE);
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void cancelDoesNotReportWhetherTheCancelWorkedTheRetainedResultTellsIt() throws Exception {
        rig = NettyRig.client();
        RequestHandle handle = send();
        long stream = awaitPeerStream(1);
        byte[] response = rig.detachedResponse(stream, Status.NOT_FOUND, BODY);

        rig.inSameStep(handle::cancel, response);

        assertOutcome(handle, Status.NOT_FOUND, Certainty.RESPONSE);
    }

    @Test
    void everyPendingResultIsPreservedBeforeTheClosedResultCompletes() throws Exception {
        rig = NettyRig.client();
        RequestHandle written = send();
        awaitPeerStream(1);
        RequestHandle unwritten = sendWithWriteLimit(0, null);

        endConnection();

        assertThat(written.result().isDone()).isTrue();
        assertThat(unwritten.result().isDone()).isTrue();
        assertOutcome(written, Status.UNAVAILABLE, Certainty.UNKNOWN);
        assertOutcome(unwritten, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }
}
