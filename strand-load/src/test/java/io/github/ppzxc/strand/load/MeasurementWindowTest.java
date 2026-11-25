package io.github.ppzxc.strand.load;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class MeasurementWindowTest {
    @Test
    void includesStartAndExcludesEnd() {
        assertThat(LoadGenRole.inMeasurementWindow(100, 100, 200)).isTrue();
        assertThat(LoadGenRole.inMeasurementWindow(199, 100, 200)).isTrue();
        assertThat(LoadGenRole.inMeasurementWindow(200, 100, 200)).isFalse();
        assertThat(LoadGenRole.inMeasurementWindow(201, 100, 200)).isFalse();
    }
}
