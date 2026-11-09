package io.github.ppzxc.strand.core.internal;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * 기한을 가진 대기 요청이 많을 때 {@code advance}의 비용(#207). 비용은 만료된 요청 수에만 비례해야 하고, 대기 요청 수({@code pending})에
 * 비례하면 안 된다.
 *
 * <ul>
 *   <li>{@link #advanceWithoutExpiry}: 아무것도 만료되지 않는 {@code advance}를 {@value #ADVANCES}번 부른다. 대기 요청 수와 무관하게 같아야
 *       한다.
 *   <li>{@link #advanceExpiringOneEach}: 기한이 1ms 간격으로 흩어진 요청에 {@code advance(1)}을 {@value #ADVANCES}번 부른다. 호출마다
 *       요청 하나가 만료된다. 만료 수에 비례하므로 대기 요청 수와 무관하게 같아야 한다.
 * </ul>
 *
 * <p>대기 요청은 쓰기 전 요청이다. 번호를 받은 요청과 AUTH 관문 전의 번호 없는 요청을 모두 쓴다.
 */
@State(Scope.Benchmark)
public class DeadlineExpiryBenchmark {

    static final int ADVANCES = 1000;
    private static final byte[] EMPTY = new byte[0];
    private static final long FAR_DEADLINE_MS = 1_000_000_000L;

    /** 번호 없는 요청이면 true, 번호를 받은 쓰기 전 요청이면 false. */
    @Param({"false", "true"})
    public boolean unnumbered;

    /** 기한을 가진 대기 요청 수. */
    @Param({"2000", "20000"})
    public int pending;

    private Endpoint endpoint;

    /** 기한이 모두 멀어 만료가 없다. 읽기만 하므로 한 번 준비한다. */
    @Setup(Level.Trial)
    public void prepareFarDeadlines() {
        endpoint = open(i -> FAR_DEADLINE_MS);
    }

    @Benchmark
    public void advanceWithoutExpiry() {
        for (int i = 0; i < ADVANCES; i++) {
            endpoint.advance(1);
        }
    }

    /** 기한이 i+1ms인 요청이 {@code pending}개 있는 상태. 시계를 밀면 소모되므로 호출마다 만든다. */
    @State(Scope.Benchmark)
    public static class Staggered {
        @Param({"false", "true"})
        public boolean unnumbered;

        @Param({"2000", "20000"})
        public int pending;

        Endpoint endpoint;

        @Setup(Level.Invocation)
        public void prepare() {
            endpoint = open(unnumbered, pending, i -> i + 1L);
        }
    }

    @Benchmark
    public void advanceExpiringOneEach(Staggered state) {
        for (int i = 0; i < ADVANCES; i++) {
            state.endpoint.advance(1);
        }
    }

    private Endpoint open(java.util.function.IntToLongFunction deadlineMs) {
        return open(unnumbered, pending, deadlineMs);
    }

    private static Endpoint open(boolean unnumbered, int pending, java.util.function.IntToLongFunction deadlineMs) {
        EndpointConfig config = EndpointConfig.defaults().maxConcurrentStreams(pending);
        Endpoint client;
        if (unnumbered) {
            client = Endpoint.client(config);
        } else {
            client = BenchSupport.establish(config)[0];
        }
        for (int i = 0; i < pending; i++) {
            client.request(BenchSupport.REQUEST_TYPE, EMPTY, deadlineMs.applyAsLong(i));
        }
        return client;
    }
}
