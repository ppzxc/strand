package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.Status;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 실제 TCP 위에서 상대가 사라지는 경우의 요청 결과와 종료 알림을 본다. 구 연결 드라이버의 TCP 시험(상대의 끝냄·재설정, 쓰기 막힘 중 즉시 끊기)을 제품 엔진 위로 옮긴
 * 것이다. 클라이언트 엔진과 서버 사이의 중계기로 상대를 끊거나 읽지 못하게 한다.
 */
class EngineTcpTest {
    private static final byte[] SMALL = new byte[10];

    private RealRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    private RequestHandle sendHeldRequest() throws Exception {
        RequestHandle handle = rig.connection.request(Request.of(AppFixture.TYPE, SMALL));
        IncomingRequest request = rig.received.poll(AppFixture.WAIT_SECONDS, TimeUnit.SECONDS);
        assertThat(request).as("서버 핸들러가 요청을 받았다").isNotNull();
        return handle;
    }

    @Test
    void whenThePeerEndsTheConnectionPendingRequestsEndAndTheNoticeCarriesTheEndOfStream() throws Exception {
        rig = new RealRig(false);
        RequestHandle pending = sendHeldRequest();

        rig.relay.killConnections();

        CloseNotice notice = rig.fixture.result(rig.connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).get().isInstanceOf(EOFException.class);
        assertThat(pending.result().isDone()).isTrue();
        assertThat(pending.result().resultNow().status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(pending.result().resultNow().certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    @Test
    void whenThePeerResetsTheConnectionPendingRequestsEndAndTheNoticeCarriesTheIoError() throws Exception {
        rig = new RealRig(false);
        RequestHandle pending = sendHeldRequest();

        rig.relay.resetConnections();

        CloseNotice notice = rig.fixture.result(rig.connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).get().isNotInstanceOf(EOFException.class);
        assertThat(pending.result().isDone()).isTrue();
        assertThat(pending.result().resultNow().status()).isEqualTo(Status.UNAVAILABLE);
        assertThat(pending.result().resultNow().certainty()).isEqualTo(Certainty.UNKNOWN);
    }

    /** 상대가 살아 있지만 읽지 않아 쓰기가 막혀 있어도 즉시 끊기는 끝나고, 스스로 끊은 연결의 알림에는 입출력 예외가 실리지 않는다. */
    @Test
    void immediateCloseEndsEvenWhileAWriteIsBlockedByAPeerThatDoesNotRead() throws Exception {
        rig = new RealRig(false);
        rig.relay.pause();
        List<RequestHandle> handles = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            handles.add(rig.connection.request(Request.of(AppFixture.TYPE, new byte[60_000])));
        }
        // 클라이언트의 쓰기가 소켓 버퍼가 차서 실제로 막힌 뒤에 끊는다.
        rig.awaitStalls(1);

        rig.connection.abort();

        CloseNotice notice = rig.fixture.result(rig.connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).isEmpty();
        for (RequestHandle handle : handles) {
            assertThat(rig.fixture.result(handle.result()).status()).isEqualTo(Status.UNAVAILABLE);
        }
    }
}
