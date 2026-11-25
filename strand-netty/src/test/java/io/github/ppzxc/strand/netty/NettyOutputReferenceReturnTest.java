package io.github.ppzxc.strand.netty;

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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NettyOutputReferenceReturnTest {
    private static final byte[] BODY = new byte[200];

    private NettyRig client;
    private NettyServerRig server;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void outgoingBytesStayReservedWhileTheFrameOfACancelledRequestIsPartiallyWrittenThenReturnOnce() throws Exception {
        client = NettyRig.client();
        client.writeCapacity(50);
        long stalls = client.stalls();
        RequestHandle handle = client.connection.request(Request.of(NettyRig.TYPE, BODY));
        NettyRig.awaitTrue(() -> client.stalls() > stalls);

        handle.cancel();
        NettyRig.awaitTrue(() -> handle.result().isDone());

        assertThat(handle.result().resultNow().status()).isEqualTo(Status.CANCELLED);
        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.UNKNOWN);
        assertThat(client.used(Resource.OUTGOING_REQUESTS)).isZero();
        assertThat(client.used(Resource.OUTGOING_BYTES)).isEqualTo(BODY.length);

        client.writeCapacity(Long.MAX_VALUE);
        NettyRig.awaitTrue(() -> client.used(Resource.OUTGOING_BYTES) == 0);

        assertThat(client.owner.usage(Resource.OUTGOING_BYTES).current()).isZero();
        NettyRig.awaitTrue(() -> client.peerResets().size() == 1);
    }

    @Test
    void outgoingBytesReturnWhenTheFrameIsWithdrawnBeforeAnyByteWasWritten() throws Exception {
        client = NettyRig.client();
        client.writeCapacity(0);
        long stalls = client.stalls();
        RequestHandle handle = client.connection.request(Request.of(NettyRig.TYPE, BODY));
        NettyRig.awaitTrue(() -> client.stalls() > stalls);

        handle.cancel();
        NettyRig.awaitTrue(() -> handle.result().isDone());

        assertThat(handle.result().resultNow().certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(client.used(Resource.OUTGOING_REQUESTS)).isZero();
        NettyRig.awaitTrue(() -> client.used(Resource.OUTGOING_BYTES) == 0);
        assertThat(client.owner.usage(Resource.OUTGOING_BYTES).current()).isZero();
    }

    @Test
    void outgoingBytesOfAPartiallyWrittenFrameReturnWhenTheConnectionEndsNotBefore() throws Exception {
        client = NettyRig.client();
        client.writeCapacity(50);
        long stalls = client.stalls();
        RequestHandle handle = client.connection.request(Request.of(NettyRig.TYPE, BODY));
        NettyRig.awaitTrue(() -> client.stalls() > stalls);
        handle.cancel();
        NettyRig.awaitTrue(() -> handle.result().isDone());
        assertThat(client.used(Resource.OUTGOING_BYTES)).isEqualTo(BODY.length);

        client.connection.abort();
        client.fixture.result(client.connection.closed());

        assertThat(client.used(Resource.OUTGOING_BYTES)).isZero();
        assertThat(client.owner.usage(Resource.OUTGOING_BYTES).current()).isZero();
    }

    @Test
    void cancellingARequestWhoseFrameIsQueuedBehindAPartiallyWrittenOneReturnsItsBytesAtOnce() throws Exception {
        client = NettyRig.client();
        client.writeCapacity(50);
        long stalls = client.stalls();
        RequestHandle writing = client.connection.request(Request.of(NettyRig.TYPE, BODY));
        NettyRig.awaitTrue(() -> client.stalls() > stalls);
        RequestHandle queued = client.connection.request(Request.of(NettyRig.TYPE, BODY));
        NettyRig.awaitTrue(() -> client.used(Resource.OUTGOING_REQUESTS) == 2);

        queued.cancel();
        NettyRig.awaitTrue(() -> queued.result().isDone());

        assertThat(queued.result().resultNow().certainty()).isEqualTo(Certainty.NOT_PROCESSED);
        assertThat(client.used(Resource.OUTGOING_REQUESTS)).isEqualTo(1);
        assertThat(client.used(Resource.OUTGOING_BYTES)).isEqualTo(BODY.length);
        assertThat(writing.result().isDone()).isFalse();
    }

    private IncomingRequest receiveOne(BlockingQueue<IncomingRequest> received) throws Exception {
        server.peerRequest(new byte[] {1});
        IncomingRequest request = received.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        assertThat(request).isNotNull();
        return request;
    }

    @Test
    void responseBytesStayReservedUntilTheAcceptedResponseFrameIsFullyWritten() throws Exception {
        BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
        server = new NettyServerRig(received::add);
        IncomingRequest request = receiveOne(received);
        server.writeCapacity(30);
        long stalls = server.stalls();

        Result<ResponseAcceptance> acceptance = request.respond(Status.OK, BODY);
        NettyServerRig.awaitTrue(() -> server.stalls() > stalls);

        assertThat(acceptance.resultNow().kind()).isEqualTo(AcceptanceKind.ACCEPTED);
        assertThat(request.ended().isDone()).isTrue();
        assertThat(server.used(Resource.INCOMING_REQUESTS)).isZero();
        assertThat(server.used(Resource.RESPONSE_BYTES)).isEqualTo(BODY.length);

        server.writeCapacity(Long.MAX_VALUE);
        NettyServerRig.awaitTrue(() -> server.used(Resource.RESPONSE_BYTES) == 0);

        assertThat(server.owner.usage(Resource.RESPONSE_BYTES).current()).isZero();
    }

    @Test
    void responseBytesOfAFrameThatNeverGotWrittenReturnWhenTheConnectionEnds() throws Exception {
        BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
        server = new NettyServerRig(received::add);
        IncomingRequest request = receiveOne(received);
        server.writeCapacity(0);
        long stalls = server.stalls();
        Result<ResponseAcceptance> acceptance = request.respond(Status.OK, BODY);
        NettyServerRig.awaitTrue(() -> server.stalls() > stalls);
        assertThat(acceptance.resultNow().kind()).isEqualTo(AcceptanceKind.ACCEPTED);
        assertThat(server.used(Resource.RESPONSE_BYTES)).isEqualTo(BODY.length);

        server.connection.abort();
        server.fixture.result(server.connection.closed());

        assertThat(server.used(Resource.RESPONSE_BYTES)).isZero();
        assertThat(server.owner.usage(Resource.RESPONSE_BYTES).current()).isZero();
    }

    @Test
    void responseBytesOfAFrameQueuedBehindTheOneBeingWrittenReturnWhenTheConnectionEnds() throws Exception {
        BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
        server = new NettyServerRig(received::add);
        IncomingRequest first = receiveOne(received);
        IncomingRequest second = receiveOne(received);
        server.writeCapacity(0);
        long stalls = server.stalls();
        first.respond(Status.OK, BODY);
        NettyServerRig.awaitTrue(() -> server.stalls() > stalls);
        Result<ResponseAcceptance> queued = second.respond(Status.OK, BODY);
        NettyServerRig.awaitTrue(queued::isDone);
        assertThat(server.used(Resource.RESPONSE_BYTES)).isEqualTo(2L * BODY.length);

        server.connection.abort();
        server.fixture.result(server.connection.closed());

        assertThat(server.used(Resource.RESPONSE_BYTES)).isZero();
        assertThat(server.owner.usage(Resource.RESPONSE_BYTES).current()).isZero();
    }

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
        AtomicInteger started = new AtomicInteger();
        HeldExecutor[] executor = new HeldExecutor[1];
        server = new NettyServerRig(request -> started.incrementAndGet(), delegate -> {
            executor[0] = new HeldExecutor(delegate);
            return executor[0];
        });
        executor[0].hold();
        long ticket = server.peerRequest(new byte[] {1, 2, 3});
        NettyServerRig.awaitTrue(() -> server.used(Resource.INCOMING_BYTES) == 3);
        assertThat(server.used(Resource.HANDLER_TASKS)).isEqualTo(1);

        server.peerCancel(ticket);
        NettyServerRig.awaitTrue(() -> server.used(Resource.INCOMING_REQUESTS) == 0);

        assertThat(server.used(Resource.INCOMING_BYTES)).isEqualTo(3);
        assertThat(server.used(Resource.HANDLER_TASKS)).isEqualTo(1);

        executor[0].runHeld();
        NettyServerRig.awaitTrue(() -> server.used(Resource.HANDLER_TASKS) == 0);

        assertThat(server.used(Resource.INCOMING_BYTES)).isZero();
        assertThat(server.owner.usage(Resource.INCOMING_BYTES).current()).isZero();
        assertThat(started.get()).isZero();
    }

    @Test
    void receivedBytesAreNotReturnedBeforeTheRequestEndsEvenAfterTheHandlerReturned() throws Exception {
        BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();
        server = new NettyServerRig(received::add);
        IncomingRequest request = receiveOne(received);
        NettyServerRig.awaitTrue(() -> server.used(Resource.HANDLER_TASKS) == 0);

        assertThat(request.ended().isDone()).isFalse();
        assertThat(server.used(Resource.INCOMING_REQUESTS)).isEqualTo(1);
        assertThat(server.used(Resource.INCOMING_BYTES)).isEqualTo(1);
    }
}
