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
public class TicketLookupBenchmark {

    static final int LOOKUPS = 1000;
    private static final byte[] EMPTY = new byte[0];
    private static final long DEADLINE_MS = 1_000_000;

    public enum Kind {
        UNNUMBERED,
        UNWRITTEN,
        WRITTEN
    }

    public enum Pattern {
        SCATTERED,
        ADJACENT
    }

    @Param({"2000", "20000", "200000"})
    public int pending;

    @Param({"UNNUMBERED", "UNWRITTEN", "WRITTEN"})
    public Kind kind;

    @Param({"SCATTERED", "ADJACENT"})
    public Pattern pattern;

    private Endpoint endpoint;

    private long[] live;

    private int rotation;

    @Setup(Level.Trial)
    public void prepare() {
        EndpointConfig config = EndpointConfig.defaults().maxConcurrentStreams(pending);
        Endpoint peer = null;
        if (kind == Kind.UNNUMBERED) {
            endpoint = Endpoint.client(config);
        } else {
            Endpoint[] pair = BenchSupport.establish(config);
            endpoint = pair[0];
            peer = pair[1];
        }
        live = new long[pending];
        for (int i = 0; i < pending; i++) {
            live[i] = endpoint.request(BenchSupport.REQUEST_TYPE, EMPTY, DEADLINE_MS);
        }
        if (kind == Kind.WRITTEN) {
            BenchSupport.pump(endpoint, peer);
        }
        verifyState(live[0]);
        verifyState(live[pending - 1]);

        cancelAndRecharge();
        for (int position = 0; position < pending; position += pending / 16) {
            verifyState(live[position]);
        }
    }

    private void verifyState(long ticket) {
        OwnRequestLedger.RequestState expected;
        if (kind == Kind.UNNUMBERED) {
            expected = OwnRequestLedger.RequestState.QUEUED_WITHOUT_NUMBER;
        } else if (kind == Kind.UNWRITTEN) {
            expected = OwnRequestLedger.RequestState.NUMBERED_UNWRITTEN;
        } else {
            expected = OwnRequestLedger.RequestState.WRITING_STARTED;
        }
        OwnRequestLedger.RequestState actual = endpoint.requestState(ticket);
        if (actual != expected) {
            throw new IllegalStateException("준비한 요청의 상태가 " + expected + "가 아니라 " + actual + "이다");
        }
    }

    private int position(int j) {
        if (pattern == Pattern.SCATTERED) {
            return (int) (((long) j * pending / LOOKUPS + rotation) % pending);
        }
        return (int) (((long) rotation * LOOKUPS + j) % pending);
    }

    @Benchmark
    public void requestStateOfSampled(Blackhole blackhole) {
        for (int j = 0; j < LOOKUPS; j++) {
            blackhole.consume(endpoint.requestState(live[position(j)]));
        }
        rotation++;
    }

    @Benchmark
    public void cancelAndRecharge() {
        for (int j = 0; j < LOOKUPS; j++) {
            int position = position(j);
            endpoint.cancelRequest(live[position]);
            live[position] = endpoint.request(BenchSupport.REQUEST_TYPE, EMPTY, DEADLINE_MS);
        }
        rotation++;

        BenchSupport.drainEvents(endpoint);
        if (kind == Kind.WRITTEN) {
            List<OutboundMessage> sent = BenchSupport.drainMessages(endpoint);
            if (sent.isEmpty()) {
                throw new IllegalStateException("내보낼 요청이 없다");
            }
        }
    }
}
