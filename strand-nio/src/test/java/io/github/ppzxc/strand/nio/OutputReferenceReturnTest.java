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

    private static final class HeldExecutor implements Executor {
        private final Executor delegate;
        private final Queue<Runnable> held = new ArrayDeque<>();
        private volatile boolean holding;
        private final java.util.concurrent.atomic.AtomicInteger delegated =
                new java.util.concurrent.atomic.AtomicInteger();

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
                delegated.incrementAndGet();
                try {
                    delegate.execute(() -> {
                        try {
                            task.run();
                        } finally {
                            delegated.decrementAndGet();
                        }
                    });
                } catch (RuntimeException | Error e) {
                    delegated.decrementAndGet();
                    throw e;
                }
            }
        }

        void hold() throws Exception {
            awaitTrue(() -> delegated.get() == 0);
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

        assertThat(request.ended().isDone()).isFalse();
        assertThat(rig.used(Resource.INCOMING_REQUESTS)).isEqualTo(1);
        assertThat(rig.used(Resource.INCOMING_BYTES)).isEqualTo(1);
    }
}
