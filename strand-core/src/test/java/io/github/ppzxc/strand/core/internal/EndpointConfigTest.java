package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 엔드포인트 설정의 기본값. */
class EndpointConfigTest {

    @Test
    void idlePingIsOnByDefaultWithA30SecondInterval() {
        assertThat(EndpointConfig.defaults().idleIntervalMs()).isEqualTo(30_000L);
    }

    @Test
    void anIdleIntervalOfZeroTurnsIdlePingOff() {
        assertThat(EndpointConfig.defaults().idleIntervalMs(0).idleIntervalMs()).isZero();
    }
}
