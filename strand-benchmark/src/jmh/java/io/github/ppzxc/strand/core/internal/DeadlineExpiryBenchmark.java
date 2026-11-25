package io.github.ppzxc.strand.core.internal;

import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

public class DeadlineExpiryBenchmark {

    static final int ADVANCES = 1000;
    private static final byte[] EMPTY = new byte[0];

    private static final long FAR_DEADLINE_MS = Long.MAX_VALUE / 2;

    @State(Scope.Benchmark)
    public abstract static class Background {

        @Param({"false", "true"})
        public boolean unnumbered;

        @Param({"2000", "20000", "200000"})
        public int pending;

        Endpoint endpoint;
        final BenchSupport.Clock clock = new BenchSupport.Clock();

        void build(int extraStreams) {

            EndpointConfig config = EndpointConfig.defaults()
                    .maxConcurrentStreams(pending + extraStreams)
                    .idleIntervalMs(0);
            endpoint = unnumbered ? Endpoint.client(config) : BenchSupport.establish(config)[0];
            for (int i = 0; i < pending; i++) {
                endpoint.request(BenchSupport.REQUEST_TYPE, EMPTY, FAR_DEADLINE_MS);
            }
            requireNoEvents();
        }

        void requireNoEvents() {
            List<Event> events = BenchSupport.drainEvents(endpoint);
            if (!events.isEmpty()) {
                throw new IllegalStateException("이벤트가 없어야 하는데 " + events.size() + "건이다: "
                        + events.get(0).kind());
            }
        }
    }

    public static class ForAdvance extends Background {
        @Setup(Level.Trial)
        public void prepare() {
            build(0);
        }

        @TearDown(Level.Trial)
        public void check() {
            requireNoEvents();
        }
    }

    public static class ForExpire extends Background {

        @Param({"10", "100", "1000"})
        public int expiring;

        @Setup(Level.Trial)
        public void prepare() {
            build(expiring);
            expireOnce(this);
        }
    }

    @Benchmark
    public void advanceWithoutExpiry(ForAdvance state) {
        for (int i = 0; i < ADVANCES; i++) {
            state.clock.advance(state.endpoint, 1);
        }
    }

    @Benchmark
    public void expireBatch(ForExpire state) {
        expireOnce(state);
    }

    private static void expireOnce(ForExpire state) {
        Endpoint endpoint = state.endpoint;
        for (int i = 0; i < state.expiring; i++) {
            endpoint.request(BenchSupport.REQUEST_TYPE, EMPTY, 1);
        }
        state.clock.advance(endpoint, 2);
        List<Event> events = BenchSupport.drainEvents(endpoint);
        if (events.size() != state.expiring) {
            throw new IllegalStateException("만료 이벤트가 " + state.expiring + "건이어야 하는데 " + events.size() + "건이다");
        }
        for (Event event : events) {
            if (event.kind() != Event.Kind.RESULT) {
                throw new IllegalStateException("만료 결과가 아닌 이벤트가 나왔다(전송이 닫혔을 수 있다): " + event.kind());
            }
        }
    }
}
