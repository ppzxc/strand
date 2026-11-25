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
public class ResponseConsumeBenchmark {

    private static final byte[] BODY = new byte[16];

    @Param({"2000", "20000", "200000"})
    public int pending;

    @Param({"OLDEST_FIRST", "NEWEST_FIRST"})
    public ResponseOrder order;

    private Endpoint client;
    private List<OutboundMessage> responses;

    @Setup(Level.Invocation)
    public void prepare() {
        Endpoint[] pair = BenchSupport.establishWithPendingRequests(pending);
        client = pair[0];
        Endpoint server = pair[1];
        BenchSupport.drainEvents(client);
        for (long streamId : BenchSupport.receivedStreamIds(server, order)) {
            server.respond(streamId, 0, BODY);
        }
        responses = BenchSupport.drainMessages(server);
        BenchSupport.expectCount("응답 프레임", pending, responses.size());
    }

    @Benchmark
    public void consumeAll() {
        BenchSupport.receiveAll(client, responses);
    }

    @TearDown(Level.Invocation)
    public void verify() {
        BenchSupport.expectCount("RESULT 이벤트", pending, BenchSupport.countEvents(client, Event.Kind.RESULT));
    }
}
