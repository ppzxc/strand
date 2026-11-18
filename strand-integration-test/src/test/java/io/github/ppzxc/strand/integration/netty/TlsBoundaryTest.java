package io.github.ppzxc.strand.integration.netty;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TlsBoundaryTest {
    @Test
    void reportsPlaintextConsumptionSeparatelyFromActualCiphertextWrite() throws Exception {
        TlsProbe.Result result = TlsProbe.exchange(new byte[] {1, 2, 3, 4});
        assertThat(result.plaintextConsumed).isEqualTo(4);
        assertThat(result.ciphertextWritten).isPositive();
        assertThat(result.received).containsExactly(1, 2, 3, 4);
    }

    @Test
    void ciphertextSocketProgressCanStopAfterPlaintextConsumption() throws Exception {
        int[] observed = TlsProbe.pressure();
        assertThat(observed[0]).isPositive(); // plaintext consumed
        assertThat(observed[1]).isPositive(); // partial ciphertext write
        assertThat(observed[2]).isEqualTo(1); // zero ciphertext write
    }
}
