package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.ErrorCode;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RequestOutcomeTest {
    private static final Duration DEADLINE = Duration.ofMillis(10);
    private static final byte[] BODY = new byte[200];

    private EngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private RequestHandle send(Duration deadline) {
        return rig.connection.request(Request.of(EngineRig.TYPE, BODY), RequestOptions.deadlineAfter(deadline));
    }

    private RequestHandle send() {
        return rig.connection.request(Request.of(EngineRig.TYPE, BODY));
    }

    private static void assertOutcome(RequestHandle handle, int status, Certainty certainty) {
        assertThat(handle.result().isDone()).isTrue();
        RequestResult result = handle.result().resultNow();
        assertThat(result.status()).isEqualTo(status);
        assertThat(result.certainty()).isEqualTo(certainty);
    }

    private RequestHandle sendWithWriteLimit(long bytes, Duration deadline) {
        long before = rig.client.writtenBytes().length;
        rig.client.writeCapacity(bytes);
        RequestHandle handle = deadline == null ? send() : send(deadline);
        rig.settle();
        assertThat(rig.client.writtenBytes().length - before).isEqualTo((int) Math.min(bytes, frameBytes()));
        return handle;
    }

    private static long frameBytes() {
        return 20 + BODY.length;
    }

    private void endConnection() throws Exception {
        rig.client.close();
        rig.settle();
        rig.fixture.result(rig.connection.closed());
    }

    @Test
    void aDeadlineOfZeroEndsAtOnceAsDeadlineExceededNotProcessedAndWritesNothing() throws Exception {
        rig = EngineRig.client();
        int before = rig.client.writtenBytes().length;

        RequestHandle handle = send(Duration.ZERO);
        rig.settle();

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED);
        assertThat(rig.client.writtenBytes()).hasSize(before);
        assertThat(rig.peerRequestStreams()).isEmpty();
        assertThat(rig.peerResets()).isEmpty();
        assertThat(rig.used(io.github.ppzxc.strand.api.Resource.OUTGOING_REQUESTS))
                .isZero();
    }

    @Test
    void aRequestWithoutADeadlineNeverExpiresNoMatterHowFarTheClockGoes() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send();
        rig.settle();

        rig.advance(Duration.ofHours(10));
        rig.settle();

        assertThat(handle.result().isDone()).isFalse();
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void theDeadlineCountsWhileTheCallWaitsForTheTransportThreadBeforeCoreRegistration() throws Exception {
        rig = EngineRig.client();
        rig.gate.closeGate();
        int before = rig.client.writtenBytes().length;

        RequestHandle handle = send(DEADLINE);
        rig.advance(DEADLINE.plusMillis(1));
        rig.gate.openGate();
        rig.settle();

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED);
        assertThat(rig.client.writtenBytes()).hasSize(before);
        assertThat(rig.peerRequestStreams()).isEmpty();
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void aRequestWhoseDeadlineHoldsWhenItIsRegisteredIsSentNormally() throws Exception {
        rig = EngineRig.client();
        rig.gate.closeGate();

        RequestHandle handle = send(DEADLINE);
        rig.advance(DEADLINE.minusMillis(5));
        rig.gate.openGate();
        rig.settle();

        assertThat(handle.result().isDone()).isFalse();
        assertThat(rig.peerRequestStreams()).hasSize(1);
    }

    @Test
    void aResponseReadInTheSameStepBeatsACancelQueuedForThatStep() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send();
        rig.settle();
        long stream = rig.peerRequestStreams().get(0);
        rig.gate.closeGate();
        rig.peer.respond(stream, Status.OK, new byte[] {7});
        rig.pumpPeer();

        handle.cancel();
        rig.gate.openGate();
        rig.settle();

        assertOutcome(handle, Status.OK, Certainty.RESPONSE);
        assertThat(handle.result().resultNow().payload()).containsExactly(7);
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void aResponseReadInTheSameStepBeatsAnExpiryOfTheSameStep() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send(DEADLINE);
        rig.settle();
        long stream = rig.peerRequestStreams().get(0);
        rig.gate.closeGate();
        rig.peer.respond(stream, Status.OK, BODY);
        rig.pumpPeer();

        rig.advance(DEADLINE.plusMillis(5));
        rig.gate.openGate();
        rig.settle();

        assertOutcome(handle, Status.OK, Certainty.RESPONSE);
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void aCancelQueuedWithAnExpiryOfTheSameStepIsDecidedByTheClockFirstAndKeepsASingleResult() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send(DEADLINE);
        rig.settle();
        rig.gate.closeGate();

        handle.cancel();
        rig.advance(DEADLINE.plusMillis(5));
        rig.gate.openGate();
        rig.settle();

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
        assertThat(rig.peerResets()).hasSize(1);
    }

    @Test
    void aLateResponseAfterACancelDoesNotChangeTheResult() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send();
        rig.settle();
        long stream = rig.peerRequestStreams().get(0);

        handle.cancel();
        rig.settle();
        rig.peerRespond(stream, Status.OK, BODY);

        assertOutcome(handle, Status.CANCELLED, Certainty.UNKNOWN);
    }

    @Test
    void aLateResponseAfterAnExpiryDoesNotChangeTheResult() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send(DEADLINE);
        rig.settle();
        long stream = rig.peerRequestStreams().get(0);

        assertThat(rig.peer.respond(stream, Status.OK, BODY)).isTrue();
        rig.advance(DEADLINE.plusMillis(5));
        rig.settle();
        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
        rig.sendPeerOutputs();

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
    }

    @Test
    void cancelBeforeAnyByteOfTheFrameWasWrittenIsCancelledNotProcessedWithoutReset() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = sendWithWriteLimit(0, null);

        handle.cancel();
        rig.client.writeCapacity(Long.MAX_VALUE);
        rig.client.wakeup();
        rig.settle();

        assertOutcome(handle, Status.CANCELLED, Certainty.NOT_PROCESSED);
        assertThat(rig.peerRequestStreams()).isEmpty();
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void cancelAfterPartialProgressFinishesTheFrameThenResetsAndIsCancelledUnknown() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = sendWithWriteLimit(50, null);

        handle.cancel();
        rig.settle();
        assertOutcome(handle, Status.CANCELLED, Certainty.UNKNOWN);
        rig.client.writeCapacity(Long.MAX_VALUE);
        rig.client.wakeup();
        rig.settle();

        assertThat(rig.peerRequestStreams()).hasSize(1);
        assertThat(rig.peerResets()).hasSize(1);
        assertThat(rig.peerResets().get(0)[0])
                .isEqualTo(rig.peerRequestStreams().get(0));
        assertThat(rig.peerEventKinds()
                        .subList(
                                rig.peerEventKinds().size() - 2,
                                rig.peerEventKinds().size()))
                .containsExactly(
                        io.github.ppzxc.strand.core.internal.Event.Kind.REQUEST_RECEIVED,
                        io.github.ppzxc.strand.core.internal.Event.Kind.REQUEST_RESET);
    }

    @Test
    void cancelAfterTheWholeFrameWasWrittenSendsResetAndIsCancelledUnknown() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send();
        rig.settle();

        handle.cancel();
        rig.settle();

        assertOutcome(handle, Status.CANCELLED, Certainty.UNKNOWN);
        assertThat(rig.peerResets()).hasSize(1);
    }

    @Test
    void expiryBeforeAnyByteOfTheFrameWasWrittenIsDeadlineExceededNotProcessedWithoutReset() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = sendWithWriteLimit(0, DEADLINE);

        rig.advance(DEADLINE.plusMillis(5));
        rig.client.writeCapacity(Long.MAX_VALUE);
        rig.settle();

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED);
        assertThat(rig.peerRequestStreams()).isEmpty();
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void expiryAfterPartialProgressFinishesTheFrameThenResetsAndIsDeadlineExceededUnknown() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = sendWithWriteLimit(50, DEADLINE);

        rig.advance(DEADLINE.plusMillis(5));
        rig.settle();
        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
        rig.client.writeCapacity(Long.MAX_VALUE);
        rig.client.wakeup();
        rig.settle();

        assertThat(rig.peerRequestStreams()).hasSize(1);
        assertThat(rig.peerResets()).hasSize(1);
    }

    @Test
    void expiryAfterTheWholeFrameWasWrittenSendsResetAndIsDeadlineExceededUnknown() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send(DEADLINE);
        rig.settle();
        long stream = rig.peerRequestStreams().get(0);

        rig.advance(DEADLINE.plusMillis(5));
        rig.settle();

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
        assertThat(rig.peerResets()).hasSize(1);
        assertThat(rig.peerResets().get(0)).containsExactly(stream, ErrorCode.CANCEL);
    }

    @Test
    void aDisconnectBeforeAnyByteOfTheFrameWasWrittenIsUnavailableNotProcessed() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = sendWithWriteLimit(0, null);

        endConnection();

        assertOutcome(handle, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }

    @Test
    void aDisconnectBeforeTheLastByteOfTheFrameWasWrittenIsUnavailableNotProcessed() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = sendWithWriteLimit(50, null);

        endConnection();

        assertOutcome(handle, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }

    @Test
    void aDisconnectAfterTheLastByteOfTheFrameWasWrittenIsUnavailableUnknown() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send();
        rig.settle();

        endConnection();

        assertOutcome(handle, Status.UNAVAILABLE, Certainty.UNKNOWN);
    }

    @Test
    void aWriteFailureAfterTheWholeFrameWasWrittenIsUnavailableUnknown() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send();
        rig.settle();

        rig.client.failWrites();
        RequestHandle next = send();
        rig.settle();

        assertOutcome(handle, Status.UNAVAILABLE, Certainty.UNKNOWN);
        assertOutcome(next, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }

    @Test
    void aWriteFailureAfterAResponseWasPreservedDoesNotChangeThatResult() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send();
        rig.settle();
        rig.peerRespond(rig.peerRequestStreams().get(0), Status.OK, new byte[] {1});

        rig.client.failWrites();
        send();
        rig.settle();

        assertOutcome(handle, Status.OK, Certainty.RESPONSE);
    }

    @Test
    void aDisconnectAfterAnExpiryDoesNotChangeThatResult() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send(DEADLINE);
        rig.settle();
        rig.advance(DEADLINE.plusMillis(5));
        rig.settle();

        endConnection();

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
    }

    @Test
    void aRequestOnAnEndedConnectionIsUnavailableNotProcessedAndCancelDoesNotMakeItCancelled() throws Exception {
        rig = EngineRig.client();
        endConnection();
        rig.fixture.result(rig.connection.closed());

        RequestHandle handle = send();
        assertOutcome(handle, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
        handle.cancel();
        rig.settle();

        assertOutcome(handle, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }

    @Test
    void cancelOfAnAnsweredRequestKeepsTheResponseAsTheRetainedResult() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send();
        rig.settle();
        rig.peerRespond(rig.peerRequestStreams().get(0), Status.OK, new byte[] {9});

        handle.cancel();
        rig.settle();

        assertOutcome(handle, Status.OK, Certainty.RESPONSE);
        assertThat(rig.peerResets()).isEmpty();
    }

    @Test
    void cancelDoesNotReportWhetherTheCancelWorkedTheRetainedResultTellsIt() throws Exception {
        rig = EngineRig.client();
        RequestHandle handle = send();
        rig.settle();
        long stream = rig.peerRequestStreams().get(0);
        rig.gate.closeGate();
        rig.peer.respond(stream, Status.NOT_FOUND, BODY);
        rig.pumpPeer();

        handle.cancel();
        rig.gate.openGate();
        rig.settle();

        assertOutcome(handle, Status.NOT_FOUND, Certainty.RESPONSE);
    }

    @Test
    void everyPendingResultIsPreservedBeforeTheClosedResultCompletes() throws Exception {
        rig = EngineRig.client();
        RequestHandle written = send();
        rig.settle();
        RequestHandle unwritten = sendWithWriteLimit(0, null);

        endConnection();

        assertThat(written.result().isDone()).isTrue();
        assertThat(unwritten.result().isDone()).isTrue();
        assertOutcome(written, Status.UNAVAILABLE, Certainty.UNKNOWN);
        assertOutcome(unwritten, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }
}
