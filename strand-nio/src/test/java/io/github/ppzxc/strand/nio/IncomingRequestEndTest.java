package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.AcceptanceKind;
import io.github.ppzxc.strand.api.RequestEnd;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import java.nio.ByteBuffer;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 받은 요청의 {@code ended()}가 응답 접수, 상대 RESET(오류 코드 포함), 연결 끝남을 구별하고(#302 결정, 규격 4.4절), 실행 대기 중에 끝난 요청의 핸들러는
 * 시작하지 않는지 본다.
 */
class IncomingRequestEndTest {
    private static final long WAIT_MS = TimeUnit.SECONDS.toMillis(AppFixture.WAIT_SECONDS);

    private final BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
    private EngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private IncomingRequest takeReceived() throws Exception {
        IncomingRequest request = received.poll(WAIT_MS, TimeUnit.MILLISECONDS);
        assertThat(request).as("핸들러가 요청을 받았다").isNotNull();
        return request;
    }

    private void awaitEnded(IncomingRequest request) throws Exception {
        long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
        while (!request.ended().isDone() && System.nanoTime() < limit) {
            Thread.sleep(5);
        }
        assertThat(request.ended().isDone()).isTrue();
    }

    private static AcceptanceKind kindOf(
            io.github.ppzxc.strand.api.Result<io.github.ppzxc.strand.api.ResponseAcceptance> result) throws Exception {
        long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
        while (!result.isDone() && System.nanoTime() < limit) {
            Thread.sleep(5);
        }
        assertThat(result.isDone()).isTrue();
        return result.resultNow().kind();
    }

    /** 상대가 RESET 프레임을 보낸 것처럼 엔진의 입력에 넣는다. 규격 2.2절 머리: 길이, type, flags, status, stream_id, 확장 길이, 예약. */
    private void injectReset(long streamId, long errorCode) {
        ByteBuffer frame = ByteBuffer.allocate(24);
        frame.putInt(20).putShort((short) 4).put((byte) 0).put((byte) 0).putLong(streamId);
        frame.putShort((short) 0).putShort((short) 0).putInt((int) errorCode);
        rig.client.inject(frame.array());
        rig.settle();
    }

    private void startServerThatDoesNotRespond() throws Exception {
        rig = EngineRig.server(received::add);
    }

    @Test
    void aPeerResetEndsTheReceivedRequestAsPeerResetWithItsErrorCode() throws Exception {
        startServerThatDoesNotRespond();
        rig.peer.request(EngineRig.TYPE, new byte[] {1});
        rig.settle();
        IncomingRequest request = takeReceived();

        injectReset(1, 0x1234);
        awaitEnded(request);

        RequestEnd end = request.ended().resultNow();
        assertThat(end.isPeerReset()).isTrue();
        assertThat(end.isResponseAccepted()).isFalse();
        assertThat(end.isConnectionEnd()).isFalse();
        assertThat(end.errorCode()).isEqualTo(OptionalLong.of(0x1234));
        assertThat(rig.used(Resource.INCOMING_REQUESTS)).isZero();
        assertThat(rig.used(Resource.INCOMING_BYTES)).isZero();
    }

    @Test
    void theCancelResetOfTheRequestingSideCarriesTheCancelCode() throws Exception {
        startServerThatDoesNotRespond();
        long ticket = rig.peer.request(EngineRig.TYPE, new byte[] {1});
        rig.settle();
        IncomingRequest request = takeReceived();

        rig.peer.cancelRequest(ticket);
        rig.settle();
        awaitEnded(request);

        assertThat(request.ended().resultNow().errorCode()).isEqualTo(OptionalLong.of(4));
    }

    @Test
    void respondingToARequestThePeerResetReportsRequestEndedAndLeavesTheEndAsPeerReset() throws Exception {
        startServerThatDoesNotRespond();
        rig.peer.request(EngineRig.TYPE, new byte[] {1});
        rig.settle();
        IncomingRequest request = takeReceived();
        injectReset(1, 4);
        awaitEnded(request);

        AcceptanceKind kind = kindOf(request.respond(Status.OK, new byte[0]));
        assertThat(kind).isEqualTo(AcceptanceKind.REQUEST_ENDED);
        assertThat(request.ended().resultNow().isPeerReset()).isTrue();
    }

    @Test
    void anAcceptedResponseEndsTheRequestAsResponseAcceptedAndALaterResetChangesNothing() throws Exception {
        startServerThatDoesNotRespond();
        rig.peer.request(EngineRig.TYPE, new byte[] {1});
        rig.settle();
        IncomingRequest request = takeReceived();

        assertThat(kindOf(request.respond(Status.OK, new byte[] {2}))).isEqualTo(AcceptanceKind.ACCEPTED);
        awaitEnded(request);
        injectReset(1, 4);

        RequestEnd end = request.ended().resultNow();
        assertThat(end.isResponseAccepted()).isTrue();
        assertThat(end.errorCode()).isEmpty();
    }

    @Test
    void aConnectionEndEndsTheRequestAsConnectionEndAndIsPreservedBeforeTheClosedResult() throws Exception {
        startServerThatDoesNotRespond();
        rig.peer.request(EngineRig.TYPE, new byte[] {1});
        rig.settle();
        IncomingRequest request = takeReceived();

        rig.client.close();
        rig.settle();
        rig.fixture.result(rig.connection.closed());

        assertThat(request.ended().isDone()).isTrue();
        assertThat(request.ended().resultNow().isConnectionEnd()).isTrue();
        assertThat(request.ended().resultNow().errorCode()).isEmpty();
        assertThat(kindOf(request.respond(Status.OK, new byte[0]))).isEqualTo(AcceptanceKind.REQUEST_ENDED);
    }

    @Test
    void aRequestThatEndedWhileWaitingForTheHandlerExecutorNeverStartsItsHandler() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger started = new AtomicInteger();
        rig = EngineRig.server(request -> {
            started.incrementAndGet();
            firstStarted.countDown();
            release.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        });
        rig.peer.request(EngineRig.TYPE, new byte[] {1});
        long second = rig.peer.request(EngineRig.TYPE, new byte[] {2});
        rig.settle();
        assertThat(firstStarted.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                .isTrue();
        assertThat(rig.used(Resource.HANDLER_TASKS)).isEqualTo(2);

        rig.peer.cancelRequest(second);
        rig.settle();
        release.countDown();

        long limit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS);
        while (rig.used(Resource.HANDLER_TASKS) != 0 && System.nanoTime() < limit) {
            Thread.sleep(5);
        }
        assertThat(rig.used(Resource.HANDLER_TASKS)).isZero();
        assertThat(started.get()).isEqualTo(1);
        assertThat(rig.used(Resource.INCOMING_REQUESTS)).isEqualTo(1);
    }
}
