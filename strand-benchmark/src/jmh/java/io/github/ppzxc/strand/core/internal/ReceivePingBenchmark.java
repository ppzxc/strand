package io.github.ppzxc.strand.core.internal;

import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * PING {@code pings}개를 서버 {@code Endpoint}가 받고 ACK를 쌓는 비용(#423). PING 수가 늘면 시간도 선형으로만 늘어야 한다. 호출이 끝나면 PING
 * 수만큼 ACK 출력이 쌓였는지 확인한다. 유휴 PING을 먼저 보내는 경로는 재지 않는다(README).
 */
@State(Scope.Benchmark)
public class ReceivePingBenchmark {

    /** 받는 PING 수. */
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
        if (frames.size() != pings) {
            throw new IllegalStateException("PING 프레임이 " + pings + "개가 아니라 " + frames.size() + "개다");
        }
    }

    @Benchmark
    public void receive() {
        for (OutboundMessage frame : frames) {
            BenchSupport.receive(server, frame.bytes());
        }
    }

    @TearDown(Level.Invocation)
    public void verify() {
        int acks = BenchSupport.drainMessages(server).size();
        if (acks != pings) {
            throw new IllegalStateException("ACK 출력이 " + pings + "개가 아니라 " + acks + "개다");
        }
    }
}
