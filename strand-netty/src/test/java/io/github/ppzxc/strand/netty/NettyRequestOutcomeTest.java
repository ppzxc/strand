package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.Event;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Netty 요청 개시자의 취소·기한·끊김이 규격(3.5절, 4.4절, 4.4.1절)의 status와 결과 확정성으로 한 번만 확정되는지 본다. 수동 시계와 쓰기량을 제한하는 실제 채널로
 * 경합을 결정적으로 만든다. 엔진의 한 걸음은 입력 처리 → 시계 → 명령 처리 순서이므로, 같은 걸음에 겹친 응답·만료·취소는 이 순서로 판정된다.
 */
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

    /** 결과가 보존될 때까지 기다린 뒤 status와 결과 확정성을 확인한다. */
    private static RequestResult assertOutcome(RequestHandle handle, int status, Certainty certainty) throws Exception {
        NettyRig.awaitTrue(() -> handle.result().isDone());
        RequestResult result = handle.result().resultNow();
        assertThat(result.status()).isEqualTo(status);
        assertThat(result.certainty()).isEqualTo(certainty);
        return result;
    }

    /** 상대가 요청을 받을 때까지 기다리고 그 스트림 번호를 돌려준다. */
    private long awaitPeerStream(int count) throws Exception {
        NettyRig.awaitTrue(() -> rig.peerRequestStreams().size() >= count);
        return rig.peerRequestStreams().get(count - 1);
    }

    /**
     * 쓰기 한도를 {@code bytes}로 두어 요청 프레임이 그만큼만 나가게 하고, 엔진이 더 쓰려다 막힌 것을 본 뒤 돌려준다. 한도 0이면 한 바이트도 나가지 않은 채 막힌다.
     */
    private RequestHandle sendWithWriteLimit(long bytes, Duration deadline) throws Exception {
        long writtenBefore = rig.written();
        long stallsBefore = rig.stalls();
        rig.writeCapacity(bytes);
        RequestHandle handle = deadline == null ? send() : send(deadline);
        NettyRig.awaitTrue(() -> rig.stalls() > stallsBefore);
        assertThat(rig.written() - writtenBefore).isEqualTo(Math.min(bytes, frameBytes()));
        return handle;
    }

    /** 이 시험이 보내는 요청 프레임의 길이: 20바이트 머리 + 본문. */
    private static long frameBytes() {
        return 20L + BODY.length;
    }

    private void releaseWrites() {
        rig.writeCapacity(Long.MAX_VALUE);
    }

    /** 상대가 연결을 끊고 종료 알림이 보존될 때까지 기다린다. */
    private void endConnection() throws Exception {
        rig.peer.disconnect();
        rig.fixture.result(rig.connection.closed());
    }

    // --- 기한: core 등록 전 대기와 0·기한 없음 ---

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

    // --- 같은 걸음에 겹친 응답·취소·만료 ---

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

    // --- 쓰기 0·부분·전체 전후의 취소 ---

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

    // --- 쓰기 0·부분·전체 전후의 기한 ---

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
        awaitPeerStream(1);

        rig.advance(DEADLINE.plusMillis(5));

        assertOutcome(handle, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
        NettyRig.awaitTrue(() -> rig.peerResets().size() == 1);
    }

    // --- 쓰기 0·부분·전체 전후의 끊김 ---

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

    // --- 확정한 결과는 늦은 사건이 바꾸지 않는다 ---

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

    // --- 끝난 연결과 cancel 호출 ---

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

        // 취소를 불렀지만 응답이 먼저 처리되어 취소는 성공하지 못했다. 호출자는 보존된 결과로만 안다.
        assertOutcome(handle, Status.NOT_FOUND, Certainty.RESPONSE);
    }

    // --- 결과 사건이 종료 사건보다 먼저 보존된다 ---

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
