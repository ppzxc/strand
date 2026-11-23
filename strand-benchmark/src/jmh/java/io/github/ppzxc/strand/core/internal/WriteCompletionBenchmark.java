package io.github.ppzxc.strand.core.internal;

import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * 프레임 하나를 다 쓰는 비용이 열린 요청 수에 비례하지 않는지 본다(#382). 응답을 기다리는 요청 {@code open}개를 열어 둔 채, 요청
 * {@value #BATCH}개를 내서 모두 쓰고(쓰기 시작과 완료), 그 요청을 취소해 RESET도 모두 쓴다. 한 호출에 프레임 {@code 2 * BATCH}개를 쓰므로
 * 쓰기 완료 처리가 열린 요청을 훑으면 시간이 {@code open}에 비례해 늘어난다.
 */
@State(Scope.Benchmark)
public class WriteCompletionBenchmark {

    static final int BATCH = 200;
    private static final byte[] EMPTY = new byte[0];

    /** 응답을 기다리며 열려 있는 요청 수. */
    @Param({"100", "2000", "20000"})
    public int open;

    private Endpoint endpoint;
    private final long[] batch = new long[BATCH];

    @Setup(Level.Trial)
    public void prepare() {
        Endpoint[] pair = BenchSupport.establish(EndpointConfig.defaults().maxConcurrentStreams(open + BATCH));
        endpoint = pair[0];
        for (int i = 0; i < open; i++) {
            endpoint.request(BenchSupport.REQUEST_TYPE, EMPTY);
        }
        BenchSupport.drainMessages(endpoint);
        BenchSupport.drainEvents(endpoint);
    }

    @Benchmark
    public int writeBatchAndCancel() {
        for (int i = 0; i < BATCH; i++) {
            batch[i] = endpoint.request(BenchSupport.REQUEST_TYPE, EMPTY);
        }
        int written = BenchSupport.drainMessages(endpoint).size();
        for (int i = 0; i < BATCH; i++) {
            endpoint.cancelRequest(batch[i]);
        }
        List<OutboundMessage> resets = BenchSupport.drainMessages(endpoint);
        BenchSupport.drainEvents(endpoint);
        return written + resets.size();
    }
}
