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

    @Test
    void immediateCloseEndsEvenWhileAWriteIsBlockedByAPeerThatDoesNotRead() throws Exception {
        rig = new RealRig(false);
        rig.relay.pause();
        List<RequestHandle> handles = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            handles.add(rig.connection.request(Request.of(AppFixture.TYPE, new byte[60_000])));
        }

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
