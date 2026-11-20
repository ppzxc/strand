package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * 실제 TCP와 TLS에서 부분 쓰기 뒤 취소된 요청의 바이트 예약이 본문을 가진 프레임이 다 나갈 때까지 유지되고(요청 수만 결과 확정 때 돌아온다) 한 번만 돌아오는지
 * 본다. TLS에서는 평문 전체 소비가 기준이다.
 */
class RealOutputReferenceTest {
    private static final byte[] BIG = new byte[900 * 1024];

    private RealRig rig;

    @AfterEach
    void tearDown() {
        if (rig != null) {
            rig.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aCancelledRequestKeepsItsBytesReservedWhileItsFrameIsPartiallyWrittenAndReturnsThemOnceTheFrameIsOut(
            boolean tls) throws Exception {
        rig = new RealRig(tls);
        rig.relay.pause();
        long stallsBefore = rig.recording.stalls.get();
        RequestHandle big = rig.connection.request(Request.of(AppFixture.TYPE, BIG));
        rig.awaitStalls(stallsBefore + 1);

        big.cancel();
        RealRig.awaitTrue(() -> big.result().isDone());

        assertThat(big.result().resultNow().status()).isEqualTo(Status.CANCELLED);
        assertThat(rig.used(Resource.OUTGOING_REQUESTS)).isZero();
        assertThat(rig.used(Resource.OUTGOING_BYTES)).isEqualTo(BIG.length);

        rig.relay.resume();
        RealRig.awaitTrue(() -> rig.used(Resource.OUTGOING_BYTES) == 0);

        assertThat(rig.used(Resource.OUTGOING_REQUESTS)).isZero();
        assertThat(rig.connection.resourceUsage(Resource.OUTGOING_BYTES).highWater())
                .isEqualTo(BIG.length);
    }
}
