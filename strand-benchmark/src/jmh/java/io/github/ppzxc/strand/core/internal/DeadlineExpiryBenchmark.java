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
 * 기한을 가진 대기 요청이 많을 때 {@code advance}의 비용(#207). 비용은 만료된 요청 수에만 비례해야 하고, 대기 요청 수({@code pending})에
 * 비례하면 안 된다.
 *
 * <p>배경으로 기한이 아주 먼 대기 요청 {@code pending}개가 깔려 있다. 이 요청은 측정 중 절대 만료되지 않는다.
 *
 * <ul>
 *   <li>{@link #advanceWithoutExpiry}: 만료가 없는 {@code advance}를 {@value #ADVANCES}번 부른다. 대기 수와 무관해야 한다. 끝날 때 이벤트가
 *       0건임을 확인한다.
 *   <li>{@link #expireBatch}: 기한 1ms인 요청 {@code expiring}개를 내고 {@code advance(2)}로 그 요청만 만료시킨다. 만료 수({@code
 *       expiring})에 비례하고 대기 수와 무관해야 한다. 한 호출 안에서 대기 수가 원래대로 돌아오므로 호출 사이에 상태를 다시 만들 필요가 없다. 잰
 *       시간은 새 요청 + 만료다. 호출마다 이벤트가 정확히 {@code expiring}건의 {@code RESULT}임을 확인한다. 전송이 닫혔다면 다른 이벤트가
 *       나오므로 이 확인이 잡는다.
 * </ul>
 *
 * <p>대기 요청은 쓰기 전 요청이다. 번호를 받은 요청과 AUTH 관문 전의 번호 없는 요청을 모두 쓴다.
 */
public class DeadlineExpiryBenchmark {

    static final int ADVANCES = 1000;
    private static final byte[] EMPTY = new byte[0];
    /** 가상 시계가 닿을 수 없는 기한. {@code now + FAR}가 넘치지 않도록 반으로 줄였다. */
    private static final long FAR_DEADLINE_MS = Long.MAX_VALUE / 2;

    /** 기한이 먼 대기 요청을 깔아 둔 엔드포인트를 만드는 공통 부분. */
    @State(Scope.Benchmark)
    public abstract static class Background {
        /** 번호 없는 요청이면 true, 번호를 받은 쓰기 전 요청이면 false. */
        @Param({"false", "true"})
        public boolean unnumbered;

        /** 기한이 먼 대기 요청 수. */
        @Param({"2000", "20000", "200000"})
        public int pending;

        Endpoint endpoint;

        void build(int extraStreams) {
            EndpointConfig config = EndpointConfig.defaults().maxConcurrentStreams(pending + extraStreams);
            endpoint = unnumbered ? Endpoint.client(config) : BenchSupport.establish(config)[0];
            for (int i = 0; i < pending; i++) {
                endpoint.request(BenchSupport.REQUEST_TYPE, EMPTY, FAR_DEADLINE_MS);
            }
            requireNoEvents();
        }

        void requireNoEvents() {
            List<Event> events = endpoint.drainEvents();
            if (!events.isEmpty()) {
                throw new IllegalStateException("이벤트가 없어야 하는데 " + events.size() + "건이다: "
                        + events.get(0).kind());
            }
        }
    }

    /** {@link #advanceWithoutExpiry}용. {@code expiring}과 무관하므로 한 번만 측정한다. */
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

    /** {@link #expireBatch}용. */
    public static class ForExpire extends Background {
        /** 한 번에 만료시키는 요청 수. */
        @Param({"10", "100", "1000"})
        public int expiring;

        @Setup(Level.Trial)
        public void prepare() {
            build(expiring);
            expireOnce(this); // 만료 수가 정확히 expiring이 아니면 던진다.
        }
    }

    @Benchmark
    public void advanceWithoutExpiry(ForAdvance state) {
        for (int i = 0; i < ADVANCES; i++) {
            state.endpoint.advance(1);
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
        endpoint.advance(2);
        List<Event> events = endpoint.drainEvents(); // 호출당 O(expiring)이다.
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
