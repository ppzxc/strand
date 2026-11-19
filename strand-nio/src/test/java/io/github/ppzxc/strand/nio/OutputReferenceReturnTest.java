package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.AcceptanceKind;
import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResponseAcceptance;
import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.api.Status;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 바이트 예약은 요청·응답 결과가 확정돼도 라이브러리 안에 그 본문을 가진 출력이 남아 있는 동안 유지되고, 실제 참조가 사라질 때 한 번만 돌아온다(#307
 * 결정: 요청 수는 결과 확정 시, 바이트는 실제 내부 참조 해제 시). 제한 쓰기 통로와 실행을 쥔 실행기로 부분 쓰기와 실행 대기를 결정적으로 만든다.
 */
class OutputReferenceReturnTest {
    private static final byte[] BODY = new byte[200];

    private EngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private static void awaitTrue(BooleanSupplier condition) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(AppFixture.WAIT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("조건이 시간 안에 참이 되지 않았다");
            }
            Thread.sleep(5);
        }
    }

    // --- 송신 요청 바이트 ---

    @Test
    void outgoingBytesStayReservedWhileTheFrameOfACancelledRequestIsPartiallyWrittenThenReturnOnce() throws Exception {
        rig = EngineRig.client();
        rig.client.writeCapacity(50);
        RequestHandle handle = rig.connection.request(Request.of(EngineRig.TYPE, BODY));
        rig.settle();

        handle.cancel();
        rig.settle();

        assertThat(handle.result().resultNow().status()).isEqualTo(Status.CANCELLED);
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.UNKNOWN);
        // 요청 수는 결과 확정 때 돌아오지만, 본문을 가진 프레임이 아직 절반만 나갔으므로 바이트는 남는다.
        assertThat(rig.used(Resource.OUTGOING_REQUESTS)).isZero();
        assertThat(rig.used(Resource.OUTGOING_BYTES)).isEqualTo(BODY.length);

        rig.client.writeCapacity(Long.MAX_VALUE);
        rig.client.wakeup();
        rig.settle();

        assertThat(rig.used(Resource.OUTGOING_BYTES)).isZero();
        assertThat(rig.owner.usage(Resource.OUTGOING_BYTES).current()).isZero();
        assertThat(rig.peerResets()).hasSize(1);
    }

    @Test
    void outgoingBytesReturnAtOnceWhenTheFrameIsWithdrawnBeforeAnyByteWasWritten() throws Exception {
        rig = EngineRig.client();
        rig.client.writeCapacity(0);
        RequestHandle handle = rig.connection.request(Request.of(EngineRig.TYPE, BODY));
        rig.settle();

        handle.cancel();
        rig.settle();

        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(rig.used(Resource.OUTGOING_REQUESTS)).isZero();
        assertThat(rig.used(Resource.OUTGOING_BYTES)).isZero();
    }

    @Test
    void outgoingBytesOfAPartiallyWrittenFrameReturnWhenTheConnectionEndsNotBefore() throws Exception {
        rig = EngineRig.client();
        rig.client.writeCapacity(50);
        RequestHandle handle = rig.connection.request(Request.of(EngineRig.TYPE, BODY));
        rig.settle();
        handle.cancel();
        rig.settle();
        assertThat(rig.used(Resource.OUTGOING_BYTES)).isEqualTo(BODY.length);

        rig.client.close();
        rig.settle();
        rig.fixture.result(rig.connection.closed());

        assertThat(rig.used(Resource.OUTGOING_BYTES)).isZero();
        assertThat(rig.owner.usage(Resource.OUTGOING_BYTES).current()).isZero();
    }

    // --- 응답 바이트 ---

    private IncomingRequest receiveOne(BlockingQueue<IncomingRequest> received) throws Exception {
        rig.peer.request(EngineRig.TYPE, new byte[] {1});
        rig.settle();
        IncomingRequest request = received.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        return request;
    }

    @Test
    void responseBytesStayReservedUntilTheAcceptedResponseFrameIsFullyWritten() throws Exception {
        BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
        rig = EngineRig.server(received::add);
        IncomingRequest request = receiveOne(received);
        rig.client.writeCapacity(30);

        Result<ResponseAcceptance> acceptance = request.respond(Status.OK, BODY);
        awaitTrue(acceptance::isDone);
        rig.settle();

        // 응답은 접수됐고 요청은 끝났지만 응답 프레임은 일부만 나갔다.
        assertThat(acceptance.resultNow().kind()).isEqualTo(AcceptanceKind.ACCEPTED);
        assertThat(request.ended().isDone()).isTrue();
        assertThat(rig.used(Resource.INCOMING_REQUESTS)).isZero();
        assertThat(rig.used(Resource.RESPONSE_BYTES)).isEqualTo(BODY.length);

        rig.client.writeCapacity(Long.MAX_VALUE);
        rig.client.wakeup();
        rig.settle();

        assertThat(rig.used(Resource.RESPONSE_BYTES)).isZero();
        assertThat(rig.owner.usage(Resource.RESPONSE_BYTES).current()).isZero();
    }

    @Test
    void responseBytesOfAFrameThatNeverGotWrittenReturnWhenTheConnectionEnds() throws Exception {
        BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
        rig = EngineRig.server(received::add);
        IncomingRequest request = receiveOne(received);
        rig.client.writeCapacity(0);
        Result<ResponseAcceptance> acceptance = request.respond(Status.OK, BODY);
        awaitTrue(acceptance::isDone);
        rig.settle();
        assertThat(rig.used(Resource.RESPONSE_BYTES)).isEqualTo(BODY.length);

        rig.client.close();
        rig.settle();
        rig.fixture.result(rig.connection.closed());

        assertThat(rig.used(Resource.RESPONSE_BYTES)).isZero();
        assertThat(rig.owner.usage(Resource.RESPONSE_BYTES).current()).isZero();
    }

    // --- 수신 요청 바이트 ---

    /** 제출은 받되 시험이 부를 때까지 실행하지 않는 실행기. */
    private static final class HeldExecutor implements Executor {
        private final Executor delegate;
        private final Queue<Runnable> held = new ArrayDeque<>();
        private volatile boolean holding;

        HeldExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable task) {
            if (holding) {
                synchronized (held) {
                    held.add(task);
                }
            } else {
                delegate.execute(task);
            }
        }

        void hold() {
            holding = true;
        }

        void runHeld() {
            holding = false;
            Runnable task;
            do {
                synchronized (held) {
                    task = held.poll();
                }
                if (task != null) {
                    delegate.execute(task);
                }
            } while (task != null);
        }
    }

    @Test
    void receivedBytesStayReservedWhileAQueuedHandlerTaskStillHoldsTheBodyEvenAfterTheRequestEnded() throws Exception {
        AppFixture fixture = AppFixture.plain();
        HeldExecutor executor = new HeldExecutor(fixture.executor);
        java.util.concurrent.atomic.AtomicInteger started = new java.util.concurrent.atomic.AtomicInteger();
        rig = EngineRig.server(request -> started.incrementAndGet(), executor);
        executor.hold();
        long ticket = rig.peer.request(EngineRig.TYPE, new byte[] {1, 2, 3});
        rig.settle();
        assertThat(rig.used(Resource.HANDLER_TASKS)).isEqualTo(1);
        assertThat(rig.used(Resource.INCOMING_BYTES)).isEqualTo(3);

        rig.peer.cancelRequest(ticket);
        rig.settle();

        // 요청은 끝나 수 예약은 돌아왔지만, 실행 대기 중인 작업이 아직 본문을 쥐고 있으므로 바이트와 작업 슬롯은 남는다.
        assertThat(rig.used(Resource.INCOMING_REQUESTS)).isZero();
        assertThat(rig.used(Resource.INCOMING_BYTES)).isEqualTo(3);
        assertThat(rig.used(Resource.HANDLER_TASKS)).isEqualTo(1);

        executor.runHeld();
        awaitTrue(() -> rig.used(Resource.HANDLER_TASKS) == 0);

        assertThat(rig.used(Resource.INCOMING_BYTES)).isZero();
        assertThat(rig.owner.usage(Resource.INCOMING_BYTES).current()).isZero();
        assertThat(started.get()).isZero();
        fixture.close();
    }

    @Test
    void receivedBytesAreNotReturnedBeforeTheRequestEndsEvenAfterTheHandlerReturned() throws Exception {
        BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
        rig = EngineRig.server(received::add);
        IncomingRequest request = receiveOne(received);
        awaitTrue(() -> rig.used(Resource.HANDLER_TASKS) == 0);

        // 핸들러 반환은 요청 종료가 아니다. 본문을 가진 받은 요청이 살아 있다.
        assertThat(request.ended().isDone()).isFalse();
        assertThat(rig.used(Resource.INCOMING_REQUESTS)).isEqualTo(1);
        assertThat(rig.used(Resource.INCOMING_BYTES)).isEqualTo(1);
    }
}
