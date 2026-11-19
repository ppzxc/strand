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

/**
 * 바이트 예약은 요청·응답 결과가 확정돼도 라이브러리 안에 그 본문을 가진 출력(core 출력 참조와 채널 쓰기용 복사본)이 남아 있는 동안 유지되고, 실제 참조가 사라질 때
 * 한 번만 돌아온다(#307 결정: 요청 수는 결과 확정 시, 바이트는 실제 내부 참조 해제 시). 쓰기량을 제한한 실제 채널과 실행을 쥔 실행기로 부분 쓰기와 실행 대기를 결정적으로
 * 만든다.
 */
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

    // --- 송신 요청 바이트 ---

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
        // 요청 수는 결과 확정 때 돌아오지만, 본문을 가진 프레임이 아직 일부만 나갔으므로 바이트는 남는다.
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
        // 철회된 프레임의 채널 쓰기용 복사본이 해제된 뒤에 바이트가 돌아온다.
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

    // --- 응답 바이트 ---

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

        // 응답은 접수됐고 요청은 끝났지만 응답 프레임은 일부만 나갔다.
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

        // 요청은 끝나 수 예약은 돌아왔지만, 실행 대기 중인 작업이 아직 본문을 쥐고 있으므로 바이트와 작업 슬롯은 남는다.
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

        // 핸들러 반환은 요청 종료가 아니다. 본문을 가진 받은 요청이 살아 있다.
        assertThat(request.ended().isDone()).isFalse();
        assertThat(server.used(Resource.INCOMING_REQUESTS)).isEqualTo(1);
        assertThat(server.used(Resource.INCOMING_BYTES)).isEqualTo(1);
    }
}
