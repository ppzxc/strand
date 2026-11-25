package io.github.ppzxc.strand.core.internal;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class ReceiveManySmallFramesBenchmark {

    private static final int MAX_FRAME_SIZE = 1_048_576;
    private static final int SMALL_FRAME_SIZE = 20;

    @Param({"131072", "1048576", "8388608"})
    public int chunkBytes;

    private Endpoint endpoint;
    private byte[] chunk;

    @Setup
    public void setup() {
        endpoint = BenchSupport.establish(EndpointConfig.defaults().maxFrameSize(MAX_FRAME_SIZE))[1];
        byte[] small = BenchSupport.reservedFrame(SMALL_FRAME_SIZE);
        chunk = new byte[chunkBytes / SMALL_FRAME_SIZE * SMALL_FRAME_SIZE];
        for (int at = 0; at < chunk.length; at += SMALL_FRAME_SIZE) {
            System.arraycopy(small, 0, chunk, at, SMALL_FRAME_SIZE);
        }

        BenchSupport.receive(endpoint, chunk);
        if (!BenchSupport.drainEvents(endpoint).isEmpty()
                || !BenchSupport.drainMessages(endpoint).isEmpty()) {
            throw new IllegalStateException("예약 프레임이 이벤트나 송신을 만들었다");
        }
    }

    @Benchmark
    public void receive() {
        BenchSupport.receive(endpoint, chunk);
    }
}
