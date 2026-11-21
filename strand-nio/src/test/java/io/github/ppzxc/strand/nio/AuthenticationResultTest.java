package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class AuthenticationResultTest {
    @Test
    void acceptedCarriesAnOptionalLifetimeInSeconds() {
        assertThat(AuthenticationResult.accepted("peer").lifetime()).isEmpty();
        assertThat(AuthenticationResult.accepted("peer", 300).lifetime()).isEqualTo(OptionalLong.of(300));
        assertThat(AuthenticationResult.accepted("peer", 0xFFFF_FFFFL).lifetime())
                .isEqualTo(OptionalLong.of(0xFFFF_FFFFL));
    }

    @Test
    void lifetimeOutsideUnsigned32BitRangeIsRejected() {
        assertThatThrownBy(() -> AuthenticationResult.accepted("peer", -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AuthenticationResult.accepted("peer", 0x1_0000_0000L))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
