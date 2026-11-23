package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.Status;
import io.github.ppzxc.strand.core.internal.Event;
import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 연결이 어떻게 끝나든 요청 결과와 종료 알림이 한 번만, 맞는 종류와 원인으로 확정되는지 본다. nio의 {@code EngineConnectionEndTest}에 대응한다. 수동 시계와 실제 채널을
 * 쓰고, 상대는 코어 엔드포인트다. 읽기 실패는 채널 파이프라인에 예외를 흘려 만들고, 상대의 CLOSE는 상대 엔드포인트가 낸 바이트를 엔진 입력에 직접 넣어 엔진이 그것을 읽는 걸음을
 * 결정한다.
 */
class NettyEngineConnectionEndTest {
    private static final byte[] BODY = new byte[20];
    private static final long FINISH_MS = 100;

    private NettyEngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private NettyEngineRig client() throws Exception {
        rig = NettyEngineRig.client(new NettyEngineRig.Tuning().closeFinish(Duration.ofMillis(FINISH_MS)));
        return rig;
    }

    private RequestHandle send() {
        return rig.connection.request(Request.of(NettyEngineRig.TYPE, BODY));
    }

    private CloseNotice notice() throws Exception {
        return rig.fixture.result(rig.connection.closed());
    }

    private RequestHandle sendAndAwaitPeer() throws Exception {
        RequestHandle handle = send();
        rig.awaitPeerRequests(1);
        return handle;
    }

    private void assertOutcome(RequestHandle handle, int status, Certainty certainty) throws Exception {
        assertThat(rig.fixture.result(handle.result()).status()).isEqualTo(status);
        assertThat(handle.result().resultNow().certainty()).isEqualTo(certainty);
    }

    @Test
    void closingTheConnectionLetsPendingStreamsFinishThenBothSidesAreToldItWasAClose() throws Exception {
        client();
        RequestHandle pending = sendAndAwaitPeer();

        rig.connection.close();
        rig.settleLoop();
        assertThat(pending.result().isDone()).isFalse();

        rig.peerRespond(rig.peerRequestStreams().get(0), 0, new byte[] {7});
        assertOutcome(pending, 0, Certainty.RESPONSE);

        NettyEngineRig.awaitTrue(rig.peer::ended);
        rig.peerHangsUp();
        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.sender()).contains(CloseNotice.Sender.LOCAL);
        assertThat(rig.peer.events()).extracting(Event::kind).contains(Event.Kind.CLOSED);
    }

    @Test
    void aRequestIssuedWhileClosingEndsWithTheUnavailableResultNotAnException() throws Exception {
        client();
        RequestHandle pending = sendAndAwaitPeer();
        rig.connection.close();
        rig.settleLoop();

        RequestHandle late = send();

        assertOutcome(late, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
        assertThat(pending.result().isDone()).isFalse();
    }

    @Test
    void cancellingWhileClosingEndsWithTheCancelledResult() throws Exception {
        client();
        RequestHandle pending = sendAndAwaitPeer();
        rig.connection.close();
        rig.settleLoop();

        pending.cancel();

        assertOutcome(pending, Status.CANCELLED, Certainty.UNKNOWN);
    }

    @Test
    void whenThePeerDropsTheConnectionTheNoticeCarriesTheEndOfStreamException() throws Exception {
        client();
        RequestHandle pending = sendAndAwaitPeer();

        rig.peerHangsUp();

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).get().isInstanceOf(EOFException.class);
        assertOutcome(pending, Status.UNAVAILABLE, Certainty.UNKNOWN);
    }

    @Test
    void whenThePeerHangsUpWhileClosingTheNoticeIsOneDisconnectWithTheEndOfStreamException() throws Exception {
        // 종료 중(CLOSE를 보냈고 스트림이 남음)에 EOF가 오면 입출력 예외가 기록된 끊김으로 한 번만 알린다.
        client();
        RequestHandle pending = sendAndAwaitPeer();
        rig.connection.close();
        rig.settleLoop();

        rig.peerHangsUp();

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).get().isInstanceOf(EOFException.class);
        assertOutcome(pending, Status.UNAVAILABLE, Certainty.UNKNOWN);
    }

    @Test
    void aReadFailureIsCarriedInTheNotice() throws Exception {
        client();
        IOException failure = new IOException("읽기 실패");

        rig.onLoop(() -> rig.channel.pipeline().fireExceptionCaught(failure));

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).containsSame(failure);
    }

    @Test
    void aWriteFailureIsCarriedInTheNotice() throws Exception {
        client();
        rig.channel.failWrites();

        send();

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).get().isInstanceOf(IOException.class);
    }

    @Test
    void afterTheCloseExchangeTheEngineClosesOnlyItsOutputAndEndsOnceThePeerHangsUp() throws Exception {
        client();

        rig.connection.close();

        // 보내는 쪽만 닫고 상대가 끊을 때까지 읽는다: 상대는 끝을 읽었지만 연결은 아직 끝나지 않았다.
        NettyEngineRig.awaitTrue(rig.peer::ended);
        assertThat(rig.closed()).isFalse();

        rig.peerHangsUp();

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.cause()).isEmpty();
    }

    @Test
    void aReadFailureAfterClosedIsNotANewNotice() throws Exception {
        client();
        rig.connection.close();
        NettyEngineRig.awaitTrue(rig.peer::ended);

        rig.onLoop(() -> rig.channel.pipeline().fireExceptionCaught(new IOException("읽기 실패")));

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.cause()).isEmpty();
    }

    @Test
    void aWriteFailureRightAfterClosedIsNeitherInTheNoticeNorANewOne() throws Exception {
        client();
        // 엔진은 상대의 CLOSE를 받아 답하며 CLOSED를 내는 같은 차례에 쓰기가 실패한다. CLOSED가 먼저 확정된다.
        rig.channel.failWrites();

        rig.inject(rig.peer.runDetached(endpoint -> endpoint.close(0)));
        rig.tick(FINISH_MS + 5);

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.sender()).contains(CloseNotice.Sender.PEER);
        assertThat(notice.cause()).isEmpty();
    }

    @Test
    void closeAndEndOfStreamReadTogetherAreOneCloseNoticeWithoutAnIoException() throws Exception {
        // 상대의 CLOSE와 EOF가 잇따라 온다. CLOSE 교환이 먼저 처리되므로 EOF는 종료 뒤의 일이다.
        client();
        rig.inject(rig.peer.runDetached(endpoint -> endpoint.close(0)));
        rig.peerHangsUp();

        CloseNotice notice = notice();

        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
        assertThat(notice.sender()).contains(CloseNotice.Sender.PEER);
        assertThat(notice.cause()).isEmpty();
    }

    @Test
    void anInputTheEndpointRejectsEndsTheConnectionAndPendingRequestsGetTheCoreResult() throws Exception {
        client();
        RequestHandle pending = sendAndAwaitPeer();

        // 길이가 머리보다 짧은 프레임은 프로토콜 위반이다.
        rig.inject(new byte[] {0, 0, 0, 1, 0, 0, 0, 0});
        rig.tick(FINISH_MS + 5);

        CloseNotice notice = notice();
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.ERROR_CLOSE);
        assertThat(notice.sender()).contains(CloseNotice.Sender.LOCAL);
        assertOutcome(pending, Status.UNAVAILABLE, Certainty.UNKNOWN);
    }

    @Test
    void aRequestOverThePeersConcurrentStreamLimitEndsAtOnceWithUnavailableAndNotProcessed() throws Exception {
        rig = NettyEngineRig.client(new NettyEngineRig.Tuning().peerConfig(config -> config.maxConcurrentStreams(1)));
        RequestHandle first = sendAndAwaitPeer();

        RequestHandle second = send();

        assertOutcome(second, Status.UNAVAILABLE, Certainty.NOT_PROCESSED);
        assertThat(first.result().isDone()).isFalse();
        assertThat(rig.closed()).isFalse();
    }
}
