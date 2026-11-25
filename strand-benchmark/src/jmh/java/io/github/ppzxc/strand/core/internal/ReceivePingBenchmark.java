package io.github.ppzxc.strand.core.internal;

import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class ReceivePingBenchmark {

    @Param({"1000", "10000", "100000"})
    public int pings;

    private Endpoint server;
    private List<OutboundMessage> frames;

    @Setup(Level.Invocation)
    public void prepare() {
        Endpoint[] pair = BenchSupport.establish(EndpointConfig.defaults());
        server = pair[1];
        byte[] payload = new byte[8];
        for (int i = 0; i < pings; i++) {
            pair[0].ping(payload);
        }
        frames = BenchSupport.drainMessages(pair[0]);
        BenchSupport.expectCount("PING 프레임", pings, frames.size());
    }

    @Benchmark
    public void receive() {
        BenchSupport.receiveAll(server, frames);
    }

    @TearDown(Level.Invocation)
    public void verify() {
        BenchSupport.expectCount(
                "ACK 출력", pings, BenchSupport.drainMessages(server).size());
    }
}
