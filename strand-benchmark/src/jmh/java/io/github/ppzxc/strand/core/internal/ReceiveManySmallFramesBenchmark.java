package io.github.ppzxc.strand.core.internal;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * 청크 하나에 작은 프레임이 가득 찬 입력의 {@code Endpoint.receive()} 비용 증가율(#200). 입력이 8배가 되면 시간도 8배 안팎(선형)이어야 한다.
 */
@State(Scope.Benchmark)
public class ReceiveManySmallFramesBenchmark {

    private static final int MAX_FRAME_SIZE = 1_048_576;
    private static final int SMALL_FRAME_SIZE = 20;

    /** 청크 하나의 크기(바이트). */
    @Param({"1048576", "8388608"})
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
        // 예약 프레임이 이벤트도 송신도 만들지 않아야 이 벤치마크가 receive만 잰다.
        endpoint.receive(chunk);
        if (!endpoint.drainEvents().isEmpty() || !endpoint.drainMessages().isEmpty()) {
            throw new IllegalStateException("예약 프레임이 이벤트나 송신을 만들었다");
        }
    }

    @Benchmark
    public void receive() {
        endpoint.receive(chunk);
    }
}
