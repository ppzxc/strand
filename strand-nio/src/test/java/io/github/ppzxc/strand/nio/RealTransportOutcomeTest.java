package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestOptions;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RealTransportOutcomeTest {
    private static final byte[] BIG = new byte[900 * 1024];
    private static final byte[] SMALL = new byte[] {1, 2, 3};
    private static final Duration SHORT_DEADLINE = Duration.ofMillis(300);

    private static final Duration LONG_DEADLINE = Duration.ofSeconds(3);

    private static final long CANCEL_CODE = 4;

    private RealRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private RequestHandle send(byte[] body) {
        return rig.connection.request(Request.of(AppFixture.TYPE, body));
    }

    private RequestHandle send(byte[] body, Duration deadline) {
        return rig.connection.request(Request.of(AppFixture.TYPE, body), RequestOptions.deadlineAfter(deadline));
    }

    private static RequestResult await(RequestHandle handle) throws Exception {
        RealRig.awaitTrue(() -> handle.result().isDone());
        return handle.result().resultNow();
    }

    private static void assertOutcome(RequestHandle handle, int status, Certainty certainty) throws Exception {
        RequestResult result = await(handle);
        assertThat(result.status()).isEqualTo(status);
        assertThat(result.certainty()).isEqualTo(certainty);
    }

    private RequestHandle blockWithAPartiallyWrittenRequest(Duration deadline) throws Exception {
        rig.relay.pause();

        long consumedBefore = rig.recording.consumed.get();
        long stallsBefore = rig.recording.stalls.get();
        RequestHandle big = deadline == null ? send(BIG) : send(BIG, deadline);
        rig.awaitStalls(stallsBefore + 1);
        assertThat(rig.recording.consumed.get() - consumedBefore).as("부분 진행").isPositive();
        return big;
    }

    private IncomingRequest takeReceived() throws Exception {
        IncomingRequest request = rig.received.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        assertThat(request).as("서버가 요청을 받았다").isNotNull();
        return request;
    }

    private void assertServerGotTheWholeFrameThenTheCancelReset() throws Exception {
        RealRig.awaitTrue(
                () -> rig.server.resourceUsage(Resource.INCOMING_BYTES).highWater() >= BIG.length);
        IncomingRequest request = rig.received.poll(1, TimeUnit.SECONDS);
        if (request != null) {
            assertThat(request.payload()).hasSize(BIG.length);
            awaitEnded(request);
            assertThat(request.ended().resultNow().isPeerReset()).isTrue();
            assertThat(request.ended().resultNow().errorCode()).isEqualTo(OptionalLong.of(CANCEL_CODE));
        }
        RealRig.awaitTrue(
                () -> rig.server.resourceUsage(Resource.INCOMING_REQUESTS).current() == 0);
    }

    private static void awaitEnded(IncomingRequest request) throws Exception {
        RealRig.awaitTrue(() -> request.ended().isDone());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancelWhileTheFrameIsPartiallyWrittenFinishesTheFrameThenResetsAndTheServerSeesTheCancelCode(boolean tls)
            throws Exception {
        rig = new RealRig(tls);
        RequestHandle big = blockWithAPartiallyWrittenRequest(null);

        big.cancel();
        assertOutcome(big, Status.CANCELLED, Certainty.UNKNOWN);
        rig.relay.resume();

        assertServerGotTheWholeFrameThenTheCancelReset();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancelOfARequestBehindAPartiallyWrittenFrameIsCancelledNotProcessedAndNeverReachesTheServer(boolean tls)
            throws Exception {
        rig = new RealRig(tls);
        RequestHandle big = blockWithAPartiallyWrittenRequest(null);
        RequestHandle queued = send(SMALL);

        queued.cancel();
        assertOutcome(queued, Status.CANCELLED, Certainty.NOT_PROCESSED);
        rig.relay.resume();

        IncomingRequest first = takeReceived();
        assertThat(first.payload()).hasSize(BIG.length);
        assertThat(big.result().isDone()).isFalse();
        assertThat(rig.received.poll(500, TimeUnit.MILLISECONDS)).isNull();
        assertThat(first.ended().isDone()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void expiryWhileTheFrameIsPartiallyWrittenIsDeadlineExceededUnknownAndTheServerStillGetsTheWholeFrameThenReset(
            boolean tls) throws Exception {
        rig = new RealRig(tls);
        RequestHandle big = blockWithAPartiallyWrittenRequest(LONG_DEADLINE);

        assertOutcome(big, Status.DEADLINE_EXCEEDED, Certainty.UNKNOWN);
        rig.relay.resume();

        assertServerGotTheWholeFrameThenTheCancelReset();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void expiryOfARequestWhoseWriteNeverStartedIsDeadlineExceededNotProcessedWithoutReset(boolean tls)
            throws Exception {
        rig = new RealRig(tls);
        blockWithAPartiallyWrittenRequest(null);
        RequestHandle queued = send(SMALL, SHORT_DEADLINE);

        assertOutcome(queued, Status.DEADLINE_EXCEEDED, Certainty.NOT_PROCESSED);
        rig.relay.resume();

        IncomingRequest first = takeReceived();
        assertThat(first.payload()).hasSize(BIG.length);
        assertThat(rig.received.poll(500, TimeUnit.MILLISECONDS)).isNull();
        assertThat(first.ended().isDone()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancelAfterTheWholeFrameWasWrittenIsCancelledUnknownAndTheServerSeesTheCancelCode(boolean tls)
            throws Exception {
        rig = new RealRig(tls);
        RequestHandle small = send(SMALL);
        IncomingRequest request = takeReceived();

        small.cancel();

        assertOutcome(small, Status.CANCELLED, Certainty.UNKNOWN);
        awaitEnded(request);
        assertThat(request.ended().resultNow().errorCode()).isEqualTo(OptionalLong.of(CANCEL_CODE));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aDisconnectEndsEveryRequestByHowFarItsFrameWasWrittenAndPreservesResultsBeforeTheClosedResult(boolean tls)
            throws Exception {
        rig = new RealRig(tls);
        RequestHandle written = send(SMALL);
        takeReceived();
        RequestHandle partial = blockWithAPartiallyWrittenRequest(null);
        RequestHandle unstarted = send(SMALL);

        rig.relay.killConnections();
        CloseNotice notice = rig.fixture.result(rig.connection.closed());

        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(written.result().isDone()).isTrue();
        assertThat(partial.result().isDone()).isTrue();
        assertThat(unstarted.result().isDone()).isTrue();
        assertOutcome(written, Status.UNAVAILABLE, Certainty.UNKNOWN);
        assertOutcome(partial, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
        assertOutcome(unstarted, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aLateResetOrCloseAfterTheDisconnectChangesNoPreservedResult(boolean tls) throws Exception {
        rig = new RealRig(tls);
        RequestHandle partial = blockWithAPartiallyWrittenRequest(null);
        rig.relay.killConnections();
        rig.fixture.result(rig.connection.closed());

        partial.cancel();
        rig.connection.abort();

        assertOutcome(partial, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
    }
}
