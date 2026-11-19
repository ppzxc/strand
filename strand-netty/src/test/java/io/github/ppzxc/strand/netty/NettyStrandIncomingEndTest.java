package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.AcceptanceKind;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestEnd;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** 받은 요청의 ended()가 상대 RESET(오류 코드 포함)을 구별한다(규격 4.4절). 응답 없는 핸들러가 붙든 요청을 상대가 취소한다. */
class NettyStrandIncomingEndTest {
    private AppFixture fixture;
    private PeerLink peer;
    private final BlockingQueue<IncomingRequest> received = new LinkedBlockingQueue<>();

    @AfterEach
    void cleanUp() {
        if (peer != null) {
            peer.close();
        }
        if (fixture != null) {
            fixture.close();
        }
    }

    /** 실제 서버(핸들러 {@code handler})에 core 클라이언트 Endpoint를 실제 소켓으로 붙이고 인증이 끝나기를 기다린다. 이 Endpoint가 요청을 낸다. */
    private void startServerWithRawClient(RequestHandler handler) throws Exception {
        fixture = AppFixture.plain();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(handler)
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        Socket socket = new Socket(
                java.net.InetAddress.getLoopbackAddress(),
                ((java.net.InetSocketAddress) listener.localAddress()).getPort());
        peer = new PeerLink(Endpoint.client(EndpointConfig.defaults().maxConcurrentStreams(8)), socket);
        peer.run(endpoint -> {});
        NettyRig.awaitTrue(() -> peer.events().stream().anyMatch(e -> e.kind() == Event.Kind.AUTHENTICATED));
    }

    private IncomingRequest takeReceived() throws Exception {
        IncomingRequest request = received.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        assertThat(request).as("핸들러가 요청을 받았다").isNotNull();
        return request;
    }

    private static AcceptanceKind kindOf(
            io.github.ppzxc.strand.api.Result<io.github.ppzxc.strand.api.ResponseAcceptance> result) throws Exception {
        NettyRig.awaitTrue(result::isDone);
        return result.resultNow().kind();
    }

    /** 상대가 RESET 프레임을 보낸 것처럼 소켓에 쓴다. 규격 2.2절 머리: 길이, type, flags, status, stream_id, 확장 길이, 예약, error_code. */
    private void sendReset(long streamId, long errorCode) throws Exception {
        ByteBuffer frame = ByteBuffer.allocate(24);
        frame.putInt(20).putShort((short) 4).put((byte) 0).put((byte) 0).putLong(streamId);
        frame.putShort((short) 0).putShort((short) 0).putInt((int) errorCode);
        peer.writeRaw(frame.array());
    }

    static Stream<String> transports() {
        return Stream.of("tcp", "tls");
    }

    @ParameterizedTest
    @MethodSource("transports")
    void aPeerCancelEndsTheReceivedRequestAsPeerResetAndReturnsItsReservations(String transport) throws Exception {
        fixture = transport.equals("tls") ? AppFixture.tls() : AppFixture.plain();
        CompletableFuture<IncomingRequest> received = new CompletableFuture<>();
        NettyStrandServer server = fixture.serverBuilder()
                .authenticate(c -> AuthenticationResult.accepted("p"))
                .handler(received::complete)
                .build();
        Listener listener = AppFixture.get(server.listen(AppFixture.anyLoopback()));
        NettyStrandConnection connection =
                AppFixture.get(fixture.clientBuilder().build().connect(AppFixture.loopback(listener)));
        RequestHandle handle = connection.request(Request.of(AppFixture.TYPE, new byte[] {1}));
        IncomingRequest request = AppFixture.get(received);

        handle.cancel();
        RequestEnd end = fixture.result(request.ended());

        assertThat(end.isPeerReset()).isTrue();
        assertThat(end.errorCode()).isEqualTo(OptionalLong.of(4));
        assertThat(request.connection()
                        .resourceUsage(Resource.INCOMING_REQUESTS)
                        .current())
                .isZero();
        connection.abort();
        listener.close();
    }

    @org.junit.jupiter.api.Test
    void aPeerResetEndsTheReceivedRequestAsPeerResetWithItsErrorCode() throws Exception {
        startServerWithRawClient(received::add);
        peer.run(endpoint -> endpoint.request(AppFixture.TYPE, new byte[] {1}));
        IncomingRequest request = takeReceived();

        sendReset(1, 0x1234);
        RequestEnd end = fixture.result(request.ended());

        assertThat(end.isPeerReset()).isTrue();
        assertThat(end.isResponseAccepted()).isFalse();
        assertThat(end.isConnectionEnd()).isFalse();
        assertThat(end.errorCode()).isEqualTo(OptionalLong.of(0x1234));
        NettyRig.awaitTrue(() ->
                request.connection().resourceUsage(Resource.INCOMING_REQUESTS).current() == 0);
        assertThat(request.connection().resourceUsage(Resource.INCOMING_BYTES).current())
                .isZero();
    }

    @org.junit.jupiter.api.Test
    void respondingToARequestThePeerResetReportsRequestEndedAndLeavesTheEndAsPeerReset() throws Exception {
        startServerWithRawClient(received::add);
        peer.run(endpoint -> endpoint.request(AppFixture.TYPE, new byte[] {1}));
        IncomingRequest request = takeReceived();
        sendReset(1, 4);
        fixture.result(request.ended());

        assertThat(kindOf(request.respond(Status.OK, new byte[0]))).isEqualTo(AcceptanceKind.REQUEST_ENDED);
        assertThat(request.ended().resultNow().isPeerReset()).isTrue();
    }

    @org.junit.jupiter.api.Test
    void anAcceptedResponseEndsTheRequestAsResponseAcceptedAndALaterResetChangesNothing() throws Exception {
        startServerWithRawClient(received::add);
        peer.run(endpoint -> endpoint.request(AppFixture.TYPE, new byte[] {1}));
        IncomingRequest request = takeReceived();

        assertThat(kindOf(request.respond(Status.OK, new byte[] {2}))).isEqualTo(AcceptanceKind.ACCEPTED);
        RequestEnd end = fixture.result(request.ended());
        sendReset(1, 4);
        assertThat(kindOf(request.respond(Status.OK, new byte[0]))).isEqualTo(AcceptanceKind.ALREADY_RESPONDED);

        assertThat(end.isResponseAccepted()).isTrue();
        assertThat(end.errorCode()).isEmpty();
        assertThat(request.ended().resultNow().isResponseAccepted()).isTrue();
    }

    @org.junit.jupiter.api.Test
    void aConnectionEndEndsTheRequestAsConnectionEndAndIsPreservedBeforeTheClosedResult() throws Exception {
        startServerWithRawClient(received::add);
        peer.run(endpoint -> endpoint.request(AppFixture.TYPE, new byte[] {1}));
        IncomingRequest request = takeReceived();

        peer.disconnect();
        fixture.result(request.connection().closed());

        assertThat(request.ended().isDone()).isTrue();
        assertThat(request.ended().resultNow().isConnectionEnd()).isTrue();
        assertThat(request.ended().resultNow().errorCode()).isEmpty();
        assertThat(kindOf(request.respond(Status.OK, new byte[0]))).isEqualTo(AcceptanceKind.REQUEST_ENDED);
    }

    @org.junit.jupiter.api.Test
    void aRequestThatEndedWhileWaitingForTheHandlerExecutorNeverStartsItsHandler() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger started = new AtomicInteger();
        startServerWithRawClient(request -> {
            received.add(request);
            started.incrementAndGet();
            firstStarted.countDown();
            release.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        });
        long[] second = new long[1];
        peer.run(endpoint -> {
            endpoint.request(AppFixture.TYPE, new byte[] {1});
            second[0] = endpoint.request(AppFixture.TYPE, new byte[] {2});
        });
        assertThat(firstStarted.await(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS))
                .isTrue();
        IncomingRequest first = takeReceived();
        NettyRig.awaitTrue(
                () -> first.connection().resourceUsage(Resource.HANDLER_TASKS).current() == 2);

        peer.run(endpoint -> endpoint.cancelRequest(second[0]));
        NettyRig.awaitTrue(() ->
                first.connection().resourceUsage(Resource.INCOMING_REQUESTS).current() == 1);
        release.countDown();

        NettyRig.awaitTrue(
                () -> first.connection().resourceUsage(Resource.HANDLER_TASKS).current() == 0);
        assertThat(started.get()).isEqualTo(1);
        assertThat(first.connection().resourceUsage(Resource.INCOMING_REQUESTS).current())
                .isEqualTo(1);
    }
}
