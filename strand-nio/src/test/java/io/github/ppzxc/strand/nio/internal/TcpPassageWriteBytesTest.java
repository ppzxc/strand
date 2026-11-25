package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class TcpPassageWriteBytesTest {

    @Test
    void reportsTheBytesActuallyWrittenFromTheGivenSlice() throws Exception {
        TcpLoopback pair = TcpLoopback.open();
        TcpPassage passage = TcpPassage.of(pair.client);
        try {
            byte[] data = {0, 1, 2, 3, 4, 5, 6, 7};

            int written = passage.writeBytes(data, 2, 4);

            assertThat(written).isEqualTo(4);
            ByteBuffer read = ByteBuffer.allocate(4);
            pair.accepted.configureBlocking(true);
            while (read.hasRemaining()) {
                pair.accepted.read(read);
            }
            assertThat(read.array()).containsExactly(2, 3, 4, 5);
        } finally {
            passage.close();
            pair.accepted.close();
        }
    }

    @Test
    void zeroLengthWritesNothing() throws Exception {
        TcpLoopback pair = TcpLoopback.open();
        TcpPassage passage = TcpPassage.of(pair.client);
        try {
            assertThat(passage.writeBytes(new byte[4], 4, 0)).isZero();
        } finally {
            passage.close();
            pair.accepted.close();
        }
    }

    @Test
    void returnsZeroWhenTheSocketBufferIsFullAndReportsPartialWritesBelowTheLength() throws Exception {
        TcpLoopback pair = TcpLoopback.open(4096);
        TcpPassage passage = TcpPassage.of(pair.client);
        try {
            byte[] big = new byte[8 * 1024 * 1024];
            int total = 0;
            boolean sawPartial = false;
            int written;
            do {
                written = passage.writeBytes(big, 0, big.length);
                total += written;
                sawPartial |= written > 0 && written < big.length;
            } while (written > 0 && total < Integer.MAX_VALUE / 2);

            assertThat(written).isZero();
            assertThat(sawPartial).isTrue();
        } finally {
            passage.close();
            pair.accepted.close();
        }
    }

    @Test
    void failsOnAClosedPassage() throws Exception {
        TcpLoopback pair = TcpLoopback.open();
        TcpPassage passage = TcpPassage.of(pair.client);
        passage.close();
        pair.accepted.close();

        assertThatThrownBy(() -> passage.writeBytes(new byte[1], 0, 1)).isInstanceOf(IOException.class);
    }
}
