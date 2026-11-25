package io.github.ppzxc.strand.core.internal;

import java.util.Arrays;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class ReceiveBigFrameBenchmark {

    private static final int MAX_FRAME_SIZE = 1_048_576;
    private static final int PIECE_SIZE = 1024;

    @Param({"32768", "131072", "1048576"})
    public int frameBytes;

    private Endpoint endpoint;
    private byte[][] pieces;

    @Setup
    public void setup() {
        endpoint = BenchSupport.establish(EndpointConfig.defaults().maxFrameSize(MAX_FRAME_SIZE))[1];
        byte[] big = BenchSupport.reservedFrame(frameBytes);
        pieces = new byte[(big.length + PIECE_SIZE - 1) / PIECE_SIZE][];
        for (int i = 0; i < pieces.length; i++) {
            pieces[i] = Arrays.copyOfRange(big, i * PIECE_SIZE, Math.min(big.length, (i + 1) * PIECE_SIZE));
        }
        for (byte[] piece : pieces) {
            BenchSupport.receive(endpoint, piece);
        }
        if (!BenchSupport.drainEvents(endpoint).isEmpty()
                || !BenchSupport.drainMessages(endpoint).isEmpty()) {
            throw new IllegalStateException("예약 프레임이 이벤트나 송신을 만들었다");
        }
    }

    @Benchmark
    public void receive() {
        for (byte[] piece : pieces) {
            BenchSupport.receive(endpoint, piece);
        }
    }
}
