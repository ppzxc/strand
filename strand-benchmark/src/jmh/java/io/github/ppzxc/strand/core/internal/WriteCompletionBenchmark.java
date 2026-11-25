package io.github.ppzxc.strand.core.internal;

import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
public class WriteCompletionBenchmark {

    static final int BATCH = 200;
    private static final byte[] EMPTY = new byte[0];

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
