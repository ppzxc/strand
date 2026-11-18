package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.core.internal.Endpoint;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;

/** requireProgress에 넘기는 최소 진행 공간이 core의 사건·출력 예약 상한과 SSLEngine 버퍼 크기에서 도출됨을 고정한다(#314 리뷰 이월 조건). */
class ProgressSpaceTest {

    private static final long MAX_FRAME = 65_536;

    @Test
    void frameAssemblyHoldsOneWholeFrame() {
        ProgressSpace space = ProgressSpace.derive(10, 5, MAX_FRAME, null);

        assertThat(space.frameAssembly()).isEqualTo(MAX_FRAME);
    }

    @Test
    void controlOutputHoldsEveryControlFrameTheEndpointMayQueueAtOnce() {
        ProgressSpace space = ProgressSpace.derive(10, 5, MAX_FRAME, null);

        // 제어 프레임 하나는 헤더(20)와 CLOSE의 고정 payload(12)를 담는 32바이트다.
        assertThat(ProgressSpace.CONTROL_OUTPUT_UNIT).isEqualTo(32);
        assertThat(space.controlOutput()).isEqualTo(Endpoint.outputsNeeded(10, 0, 5) * 32);
        assertThat(ProgressSpace.outputSlots(space.controlOutput())).isEqualTo(Endpoint.outputsNeeded(10, 0, 5));
    }

    @Test
    void resultEventSpaceHoldsOneResponseBodyPlusHeaderSizedLocalResults() {
        ProgressSpace space = ProgressSpace.derive(10, 5, MAX_FRAME, null);

        assertThat(space.resultEvent()).isEqualTo(MAX_FRAME + (10 + 1) * Endpoint.FRAME_HEADER_SIZE);
        assertThat(ProgressSpace.eventSlots(space.resultEvent(), MAX_FRAME)).isEqualTo(Endpoint.eventsNeeded(10));
    }

    @Test
    void largerConfiguredSpaceGivesMoreSlots() {
        assertThat(ProgressSpace.eventSlots(MAX_FRAME + 20 * 100, MAX_FRAME)).isEqualTo(101);
        assertThat(ProgressSpace.outputSlots(32 * 100)).isEqualTo(100);
    }

    @Test
    void slotCountsSaturateInsteadOfOverflowing() {
        assertThat(ProgressSpace.eventSlots(Long.MAX_VALUE, MAX_FRAME)).isEqualTo(Integer.MAX_VALUE);
        assertThat(ProgressSpace.outputSlots(Long.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void spaceBelowOneFrameStillGivesOneEventSlot() {
        assertThat(ProgressSpace.eventSlots(10, MAX_FRAME)).isEqualTo(1);
    }

    @Test
    void withoutTlsTheTlsSpacesAreNotReservedButKeepAPositiveMinimum() {
        ProgressSpace space = ProgressSpace.derive(10, 5, MAX_FRAME, null);

        assertThat(space.tlsPlaintext()).isZero();
        assertThat(space.tlsCiphertext()).isZero();
        assertThat(space.tlsControl()).isZero();
        assertThat(space.minimums()).containsExactly(MAX_FRAME, 32 * 19, MAX_FRAME + 220, 1, 1, 1);
    }

    @Test
    void tlsSpacesComeFromTheEngineBufferSizes() throws Exception {
        TlsSizes sizes = TlsSizes.of(SSLContext.getDefault());

        ProgressSpace space = ProgressSpace.derive(10, 5, MAX_FRAME, sizes);

        // 복호한 평문 한 레코드, 암호문(받는 쪽 둘 + 보내는 쪽 데이터 하나), 핸드셰이크·경고 출력 한 패킷.
        assertThat(space.tlsPlaintext()).isEqualTo(sizes.applicationBufferSize());
        assertThat(space.tlsCiphertext()).isEqualTo(3L * sizes.packetBufferSize());
        assertThat(space.tlsControl()).isEqualTo(sizes.packetBufferSize());
        assertThat(sizes.applicationBufferSize()).isGreaterThanOrEqualTo(16_384);
        assertThat(sizes.packetBufferSize()).isGreaterThanOrEqualTo(16_384);
    }

    @Test
    void requireRejectsLimitsBelowTheDerivedMinimumOnEitherScope() {
        ProgressSpace space = ProgressSpace.derive(10, 5, MAX_FRAME, null);

        space.require(limits(space, 1, 1));
        assertThatThrownBy(() -> space.require(limits(space, 0, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RESULT_EVENT_BYTES");
        assertThatThrownBy(() -> space.require(limits(space, 1, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CONTROL_OUTPUT_BYTES");
    }

    /** {@code connectionShortfall}가 0이면 연결 범위의 결과 사건 공간이, {@code ownerShortfall}이 0이면 소유 객체의 제어 출력 공간이 하나 모자란다. */
    private static ResourceLimits limits(ProgressSpace space, int connectionShortfall, int ownerShortfall) {
        long[] min = space.minimums();
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(10, 1000)
                        .incomingRequests(5, 1000)
                        .responseBytes(1000)
                        .handlerTasks(5)
                        .observations(5)
                        .preparationBytes(1000)
                        .progressBytes(min[0], min[1], min[2] - (1 - connectionShortfall), min[3], min[4], min[5])
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(5)
                        .totalConnections(5)
                        .preparationTasks(5, 5, 5)
                        .outgoingRequests(10, 1000)
                        .incomingRequests(5, 1000)
                        .responseBytes(1000)
                        .handlerTasks(5)
                        .observations(5)
                        .preparationBytes(1000)
                        .progressBytes(min[0], min[1] - (1 - ownerShortfall), min[2], min[3], min[4], min[5])
                        .build())
                .build();
    }
}
