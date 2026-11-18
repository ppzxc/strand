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

    @Test
    void ciphertextTravelsThroughTheExperimentalNettyOutboundPath() throws Exception {
        TlsProbe.Result result = TlsProbe.exchangeOutbound(new byte[] {5, 6, 7});
        assertThat(result.plaintextConsumed).isEqualTo(3);
        assertThat(result.ciphertextWritten).isPositive();
        assertThat(result.received).containsExactly(5, 6, 7);
    }

    @Test
    void nettyOutboundReportsTlsCiphertextBackpressureAfterPlaintextConsumption() throws Exception {
        int[] observed = TlsProbe.pressureOutbound();
        assertThat(observed[0]).isPositive();
        assertThat(observed[1]).isPositive();
        assertThat(observed[2]).isEqualTo(1);
    }

    @Test
    void partialPlaintextConsumptionCanFinishRequestBeforeReset() throws Exception {
        byte[] request = new byte[32768];
        java.nio.ByteBuffer.wrap(request)
                .putInt(request.length - 4)
                .putShort((short) 0x0101)
                .put((byte) 0)
                .put((byte) 0)
                .putLong(1)
                .putInt(0);
        byte[] reset = java.nio.ByteBuffer.allocate(24)
                .putInt(20)
                .putShort((short) 4)
                .put((byte) 0)
                .put((byte) 0)
                .putLong(1)
                .putInt(0)
                .putInt(4)
                .array();
        TlsProbe.Result result = TlsProbe.exchangePartial(request, reset);
        assertThat(result.plaintextConsumed).isPositive().isLessThan(request.length);
        byte[] expected = java.util.Arrays.copyOf(request, request.length + reset.length);
        System.arraycopy(reset, 0, expected, request.length, reset.length);
        assertThat(result.received).containsExactly(expected);
    }

    @Test
    void plaintextConsumptionPrecedesTlsAndTransportCleanup() throws Exception {
        TlsProbe.CloseResult result = TlsProbe.exchangeAndClose(new byte[] {9, 8, 7});
        assertThat(result.plaintextConsumed).isEqualTo(3);
        assertThat(result.tlsOpenAfterConsumption).isTrue();
        assertThat(result.transportOpenAfterConsumption).isTrue();
        assertThat(result.peerReceived).containsExactly(9, 8, 7);
        assertThat(result.tlsClosed).isTrue();
        assertThat(result.transportClosed).isTrue();
    }
}
