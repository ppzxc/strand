package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class WriteQueueLimitTest {

    private static final long PLENTY = 1L << 40;

    @Test
    void theLimitIsTheFlowControlRangePlusOneFrame() {

        assertThat(WriteQueueLimit.of(65_536, 100, 100, PLENTY)).isEqualTo(200L * 65_536 + 65_536);
        assertThat(WriteQueueLimit.of(16_384, 2, 2, PLENTY)).isEqualTo(5L * 16_384);

        assertThat(WriteQueueLimit.of(16_384, 0, 0, PLENTY)).isEqualTo(16_384);
    }

    @Test
    void aPeerCannotRaiseTheLimitWithoutBoundByAdvertisingHugeValues() {
        long hugeStreams = 4_294_967_295L;
        long hugeFrame = 1_048_576L;
        assertThat(WriteQueueLimit.of(hugeFrame, hugeStreams, hugeStreams, WriteQueueLimit.DEFAULT_ABSOLUTE_LIMIT))
                .isEqualTo(WriteQueueLimit.DEFAULT_ABSOLUTE_LIMIT);

        assertThat(WriteQueueLimit.of(hugeFrame, 100, 100, PLENTY)).isEqualTo(201L * hugeFrame);
    }

    @Test
    void theLimitIsNeverBelowOneFrameEvenWithASmallAbsoluteLimit() {
        assertThat(WriteQueueLimit.of(1_048_576, 100, 100, 1)).isEqualTo(1_048_576);
    }
}
