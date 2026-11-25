package io.github.ppzxc.strand.core.internal;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class RespondBenchmark {

    private static final byte[] BODY = new byte[16];

    @Param({"2000", "20000", "200000"})
    public int pending;

    @Param({"OLDEST_FIRST", "NEWEST_FIRST"})
    public ResponseOrder order;

    private Endpoint server;
    private long[] streamIds;

    @Setup(Level.Invocation)
    public void prepare() {
        server = BenchSupport.establishWithPendingRequests(pending)[1];
        streamIds = BenchSupport.receivedStreamIds(server, order);
        BenchSupport.expectCount("받은 요청", pending, streamIds.length);
    }

    @Benchmark
    public void respondToAll() {
        for (long streamId : streamIds) {
            server.respond(streamId, 0, BODY);
        }
    }

    @TearDown(Level.Invocation)
    public void verify() {
        BenchSupport.expectCount(
                "응답 송신", pending, BenchSupport.drainMessages(server).size());
    }
}
