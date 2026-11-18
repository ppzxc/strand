package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SaturatingTest {

    @Test
    void addsWithoutOverflowAndStopsAtTheLimits() {
        assertThat(Saturating.add(1, 2)).isEqualTo(3);
        assertThat(Saturating.add(Long.MAX_VALUE - 1, 5)).isEqualTo(Long.MAX_VALUE);
        assertThat(Saturating.add(Long.MIN_VALUE + 1, -5)).isEqualTo(Long.MIN_VALUE);
        assertThat(Saturating.add(-5, Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE - 5);
    }
}
