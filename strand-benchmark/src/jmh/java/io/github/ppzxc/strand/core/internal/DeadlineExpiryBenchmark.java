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
 *   <li>{@link #advanceWithoutExpiry}: 만료가 없는 {@code advance}를 {@value #ADVANCES}번 부른다. 대기 수와 무관해야 한다. 끝날 때 만료
 *       이벤트가 0건이고 전송이 닫히지 않았음을 확인한다.
 *   <li>{@link #expireBatch}: 기한 1ms인 요청 {@code expiring}개를 내고 {@code advance(2)}로 그 요청만 만료시킨다. 만료 수({@code
 *       expiring})에 비례하고 대기 수와 무관해야 한다. 한 호출 안에서 대기 수가 원래대로 돌아오므로 호출 사이에 상태를 다시 만들 필요가 없다. 잰
 *       시간은 새 요청 + 만료다. 호출마다 만료 이벤트가 정확히 {@code expiring}건이고 전송이 닫히지 않았음을 확인한다.
 * </ul>
 *
 * <p>대기 요청은 쓰기 전 요청이다. 번호를 받은 요청과 AUTH 관문 전의 번호 없는 요청을 모두 쓴다. 번호 없는 쪽은 {@code advance}가 AUTH 제한
 * 시간으로 전송을 닫지 않는지(닫으면 이벤트가 달라진다)도 같은 확인으로 잡는다.
 */
@State(Scope.Benchmark)
public class DeadlineExpiryBenchmark {

    static final int ADVANCES = 1000;
    private static final byte[] EMPTY = new byte[0];
    /** 가상 시계가 닿을 수 없는 기한. {@code now + FAR}가 넘치지 않도록 반으로 줄였다. */
    private static final long FAR_DEADLINE_MS = Long.MAX_VALUE / 2;

    /** 번호 없는 요청이면 true, 번호를 받은 쓰기 전 요청이면 false. */
    @Param({"false", "true"})
    public boolean unnumbered;

    /** 기한이 먼 대기 요청 수. */
    @Param({"2000", "20000", "200000"})
    public int pending;

    /** {@link #expireBatch}가 한 번에 만료시키는 요청 수. */
    @Param({"10", "100", "1000"})
    public int expiring;

    private Endpoint endpoint;

    @Setup(Level.Trial)
    public void prepare() {
        EndpointConfig config = EndpointConfig.defaults().maxConcurrentStreams(pending + expiring);
        endpoint = unnumbered ? Endpoint.client(config) : BenchSupport.establish(config)[0];
        for (int i = 0; i < pending; i++) {
            endpoint.request(BenchSupport.REQUEST_TYPE, EMPTY, FAR_DEADLINE_MS);
        }
        // 준비가 끝났을 때 이벤트가 없어야 한다. 한 번 돌려 만료 수와 닫힘 여부도 미리 본다.
        requireEvents(0);
        expireBatch(); // 만료 수가 정확히 expiring이 아니면 던진다.
        requireEvents(0);
    }

    @TearDown(Level.Trial)
    public void check() {
        requireEvents(0); // advanceWithoutExpiry만 돌았다면 측정 내내 이벤트가 없어야 한다. expireBatch는 호출마다 비운다.
    }

    private void requireEvents(int expectedResults) {
        List<Event> events = endpoint.drainEvents();
        if (events.size() != expectedResults) {
            throw new IllegalStateException("이벤트가 " + expectedResults + "건이어야 하는데 " + events.size() + "건이다");
        }
        for (Event event : events) {
            if (event.kind() != Event.Kind.RESULT) {
                throw new IllegalStateException("만료 결과가 아닌 이벤트가 나왔다(전송이 닫혔을 수 있다): " + event.kind());
            }
        }
    }

    @Benchmark
    public void advanceWithoutExpiry() {
        for (int i = 0; i < ADVANCES; i++) {
            endpoint.advance(1);
        }
    }

    @Benchmark
    public void expireBatch() {
        for (int i = 0; i < expiring; i++) {
            endpoint.request(BenchSupport.REQUEST_TYPE, EMPTY, 1);
        }
        endpoint.advance(2);
        List<Event> events = endpoint.drainEvents(); // 호출당 O(expiring)이다.
        if (events.size() != expiring) {
            throw new IllegalStateException("만료 이벤트가 " + expiring + "건이어야 하는데 " + events.size() + "건이다");
        }
    }
}
