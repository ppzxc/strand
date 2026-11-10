package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class BytesTest {

    @Test
    void fromHexIgnoresWhitespace() {
        assertThat(Bytes.fromHex("0a 0B\n0c")).containsExactly(0x0a, 0x0b, 0x0c);
    }

    @Test
    void fromHexRejectsAnOddNumberOfDigitsInsteadOfDroppingTheLast() {
        assertThatThrownBy(() -> Bytes.fromHex("0a0")).isInstanceOf(IllegalArgumentException.class);
    }
}
