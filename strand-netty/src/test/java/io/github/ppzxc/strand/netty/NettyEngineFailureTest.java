package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 채널이 예기치 못한 오류를 내도 연결이 끝나고, 결과가 보존되며, 예약이 돌아오는지 본다. nio의 {@code ConnectionEngineFailureTest}에 대응한다(채널을 직접 꽂아 엔진을
 * 시험한다).
 */
class NettyEngineFailureTest {
    private NettyEngineRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    @Test
    void anUnexpectedErrorFromTheChannelEndsTheConnectionAndKeepsTheCauseAsTheClosedResult() throws Exception {
        rig = NettyEngineRig.client();
        UnsupportedOperationException boom = new UnsupportedOperationException("쓰기를 지원하지 않는 채널");
        rig.channel.failWritesUnexpectedly(boom);

        RequestHandle pending = rig.connection.request(Request.of(NettyEngineRig.TYPE, new byte[20]));

        CloseNotice notice = rig.fixture.result(rig.connection.closed());
        assertThat(notice.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
        assertThat(notice.cause()).containsSame(boom);
        assertThat(rig.fixture.result(pending.result()).status()).isEqualTo(Status.UNAVAILABLE);
        NettyEngineRig.awaitTrue(
                () -> rig.owner.usage(Resource.TOTAL_CONNECTIONS).current() == 0);
        assertThat(rig.owner.usage(Resource.PREPARING_CONNECTIONS).current()).isZero();
    }
}
