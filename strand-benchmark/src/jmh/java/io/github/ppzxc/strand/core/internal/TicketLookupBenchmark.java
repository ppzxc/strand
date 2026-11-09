package io.github.ppzxc.strand.core.internal;

import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * 번호표로 요청을 찾는 비용(#207). 대기 요청 수({@code pending})가 늘어도 같은 개수({@value #LOOKUPS})를 찾는 시간은 그대로여야 한다(O(1)).
 * 대기 수 점을 셋 이상 둬서 기울기를 본다.
 *
 * <p>요청의 상태({@link Kind}):
 *
 * <ul>
 *   <li>{@code UNNUMBERED}: AUTH 관문 전에 낸 요청. 스트림 번호가 없는 대기열 요청이다.
 *   <li>{@code UNWRITTEN}: 번호를 받았고 아직 쓰기 대기열에서 나가지 않은(쓰기 전) 요청이다.
 *   <li>{@code WRITTEN}: 번호를 받고 쓰기를 시작한 요청이다. 응답을 기다린다.
 * </ul>
 *
 * <p>찾는 번호표의 분포({@link Pattern}): {@code SCATTERED}는 낸 순서 전체에 고르게 퍼뜨린 표본이고, {@code ADJACENT}는 서로 이웃한 번호표
 * 묶음(대조군)이다. 두 분포의 비용이 같고 둘 다 대기 수와 무관해야 한다.
 *
 * <p>취소({@link #cancelAndRecharge})는 번호표 {@value #LOOKUPS}개를 취소한 즉시 같은 자리에 새 요청을 내서 대기 수를 그대로 유지한다. 그래서
 * 호출 사이에 상태를 다시 만들 필요가 없고({@code Level.Invocation} 안 씀) 한 호출이 수십 마이크로초 이상 걸린다. 잰 시간은 취소 + 새 요청
 * 낸 값이다. 새 요청 비용도 대기 수와 무관해야 하므로 대기 수에 따른 변화는 그대로 읽는다.
 */
@State(Scope.Benchmark)
public class TicketLookupBenchmark {

    static final int LOOKUPS = 1000;
    private static final byte[] EMPTY = new byte[0];
    private static final long DEADLINE_MS = 1_000_000;

    /** 요청의 상태. */
    public enum Kind {
        UNNUMBERED,
        UNWRITTEN,
        WRITTEN
    }

    /** 찾는 번호표의 분포. */
    public enum Pattern {
        SCATTERED,
        ADJACENT
    }

    /** 대기 요청 수. */
    @Param({"2000", "20000", "200000"})
    public int pending;

    @Param({"UNNUMBERED", "UNWRITTEN", "WRITTEN"})
    public Kind kind;

    @Param({"SCATTERED", "ADJACENT"})
    public Pattern pattern;

    private Endpoint endpoint;
    /** 위치별 현재 번호표. 취소하고 새로 내면 같은 위치에 새 번호표를 넣는다. */
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
        // 한 번 돌려 본다. 취소 뒤 새로 낸 요청도 같은 상태이고, 대기 수가 그대로여야 한다.
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

    /** {@code j}번째 표본의 위치. */
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
        // 취소가 쌓은 이벤트와, 쓰기 시작 상태를 유지하려면 새 요청을 내보내야 한다. 둘 다 호출당 O(LOOKUPS)다.
        endpoint.drainEvents();
        if (kind == Kind.WRITTEN) {
            List<OutboundMessage> sent = endpoint.drainMessages();
            if (sent.isEmpty()) {
                throw new IllegalStateException("내보낼 요청이 없다");
            }
        }
    }
}
