package io.github.ppzxc.strand.core.internal;

import java.util.Arrays;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * 큰 프레임 하나가 작은 조각으로 도착하는 입력의 {@code Endpoint.receive()} 비용 증가율(#200). 프레임이 8배가 되면 시간도 8배 안팎(선형)이어야
 * 한다. 수정 전에는 조각마다 쌓인 전체를 이어 붙여 이차다.
 */
@State(Scope.Benchmark)
public class ReceiveBigFrameBenchmark {

    private static final int MAX_FRAME_SIZE = 1_048_576;
    private static final int PIECE_SIZE = 1024;

    /** 큰 프레임 하나의 크기(바이트, 길이 필드 포함). */
    @Param({"131072", "1048576"})
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
            endpoint.receive(piece);
        }
        if (!endpoint.drainEvents().isEmpty() || !endpoint.drainMessages().isEmpty()) {
            throw new IllegalStateException("예약 프레임이 이벤트나 송신을 만들었다");
        }
    }

    @Benchmark
    public void receive() {
        for (byte[] piece : pieces) {
            endpoint.receive(piece);
        }
    }
}
