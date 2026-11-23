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

    @Test
    void validatorsExposeTheRangesTheSettersEnforce() {
        assertThat(EndpointConfig.isValidIdleIntervalMs(0)).isTrue();
        assertThat(EndpointConfig.isValidIdleIntervalMs(-1)).isFalse();
        assertThat(EndpointConfig.isValidIdleIntervalMs(Long.MAX_VALUE / 2 + 1)).isFalse();
        assertThat(EndpointConfig.isValidAckTimeoutMs(0)).isFalse();
        assertThat(EndpointConfig.isValidAckTimeoutMs(1)).isTrue();
        assertThat(EndpointConfig.isValidAckTimeoutMs(Long.MAX_VALUE / 2)).isTrue();
        assertThat(EndpointConfig.isUnsigned32(0)).isTrue();
        assertThat(EndpointConfig.isUnsigned32(0xFFFF_FFFFL)).isTrue();
        assertThat(EndpointConfig.isUnsigned32(0x1_0000_0000L)).isFalse();
        assertThat(EndpointConfig.isUnsigned32(-1)).isFalse();
    }
}
