package io.github.ppzxc.strand.core.internal;

import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class OutboxBenchmark {

    private static final byte[] EMPTY = new byte[0];
    private static final long DEADLINE_MS = 1_000_000;

    @Param({"2000", "20000"})
    public int requests;

    private Endpoint server;
    private BenchSupport.Clock clock;
    private long[] tickets;

    @Setup(Level.Invocation)
    public void prepare() {
        Endpoint[] pair = BenchSupport.establish(EndpointConfig.defaults().maxConcurrentStreams(requests));
        Endpoint client = pair[0];
        server = pair[1];
        clock = new BenchSupport.Clock();
        for (int i = 0; i < requests; i++) {
            client.request(BenchSupport.REQUEST_TYPE, EMPTY);
        }
        BenchSupport.pump(client, server);
        tickets = new long[requests];
        for (int i = 0; i < requests; i++) {
            tickets[i] = server.request(BenchSupport.REQUEST_TYPE, EMPTY, DEADLINE_MS);
        }
    }

    @Benchmark
    public List<OutboundMessage> closeWithQueuedRequests() {
        server.close(ErrorCode.PROTOCOL_ERROR);
        return BenchSupport.drainMessages(server);
    }

    @Benchmark
    public void cancelAllByTicket() {
        for (int i = 0; i < requests; i++) {
            server.cancelRequest(tickets[i]);
        }
    }

    @Benchmark
    public void cancelAllByTicketNewestFirst() {
        for (int i = requests - 1; i >= 0; i--) {
            server.cancelRequest(tickets[i]);
        }
    }

    @Benchmark
    public void requestStateOfEveryTicket(Blackhole blackhole) {
        for (int i = 0; i < requests; i++) {
            blackhole.consume(server.requestState(tickets[i]));
        }
    }

    @Benchmark
    public void advanceOncePerPendingRequest() {
        for (int i = 0; i < requests; i++) {
            clock.advance(server, 1);
        }
    }
}
