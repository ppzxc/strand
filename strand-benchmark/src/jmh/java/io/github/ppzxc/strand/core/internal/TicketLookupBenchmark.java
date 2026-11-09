package io.github.ppzxc.strand.core.internal;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * 번호표로 요청을 찾는 비용(#207). 대기 요청 수({@code pending})가 늘어도 같은 개수({@value #LOOKUPS})를 찾는 시간은 그대로여야 한다(O(1)).
 *
 * <p>찾는 번호표는 낸 순서 전체에 고르게 퍼뜨린 {@value #LOOKUPS}개다. 앞쪽만 찾으면 선형 탐색이 가려진다. 대기 요청은 모두 기한을 갖고 있다.
 *
 * <ul>
 *   <li>{@link Kind#UNNUMBERED}: AUTH 관문 전에 낸 요청. 스트림 번호가 없는 대기열 요청이다.
 *   <li>{@link Kind#UNWRITTEN}: 번호를 받았고 아직 쓰기 대기열에서 나가지 않은(쓰기 전) 요청이다.
 *   <li>{@link Kind#WRITTEN}: 번호를 받고 쓰기를 시작한 요청이다. 응답을 기다린다.
 * </ul>
 *
 * <p>{@link #requestStateOfSampledTickets}는 읽기만 하므로 시험 시작 때 한 번 준비한다. {@link #cancelSampledTickets}는 요청을 없애므로
 * 호출마다 준비한다({@code Level.Invocation}, 준비 시간은 재지 않는다).
 */
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

    /** 대기 요청을 준비하고 찾을 번호표를 고르는 공통 부분. */
    @State(Scope.Benchmark)
    public abstract static class Pending {
        /** 대기 요청 수. */
        @Param({"2000", "20000"})
        public int pending;

        @Param({"UNNUMBERED", "UNWRITTEN", "WRITTEN"})
        public Kind kind;

        Endpoint endpoint;
        long[] sampled;

        void build() {
            EndpointConfig config = EndpointConfig.defaults().maxConcurrentStreams(pending);
            Endpoint client;
            Endpoint peer = null;
            if (kind == Kind.UNNUMBERED) {
                client = Endpoint.client(config);
            } else {
                Endpoint[] pair = BenchSupport.establish(config);
                client = pair[0];
                peer = pair[1];
            }
            long[] tickets = new long[pending];
            for (int i = 0; i < pending; i++) {
                tickets[i] = client.request(BenchSupport.REQUEST_TYPE, EMPTY, DEADLINE_MS);
            }
            if (kind == Kind.WRITTEN) {
                BenchSupport.pump(client, peer);
            }
            endpoint = client;
            sampled = new long[LOOKUPS];
            for (int i = 0; i < LOOKUPS; i++) {
                sampled[i] = tickets[(int) ((long) i * pending / LOOKUPS)];
            }
            verifyState(sampled[0]);
            verifyState(sampled[LOOKUPS - 1]);
        }

        private void verifyState(long ticket) {
            Endpoint.RequestState expected;
            if (kind == Kind.UNNUMBERED) {
                expected = Endpoint.RequestState.QUEUED_WITHOUT_NUMBER;
            } else if (kind == Kind.UNWRITTEN) {
                expected = Endpoint.RequestState.NUMBERED_UNWRITTEN;
            } else {
                expected = Endpoint.RequestState.WRITING_STARTED;
            }
            Endpoint.RequestState actual = endpoint.requestState(ticket);
            if (actual != expected) {
                throw new IllegalStateException("준비한 요청의 상태가 " + expected + "가 아니라 " + actual + "이다");
            }
        }
    }

    /** 읽기만 하므로 시험 시작 때 한 번 준비한다. */
    @State(Scope.Benchmark)
    public static class ForRead extends Pending {
        @Setup(Level.Trial)
        public void prepare() {
            build();
        }
    }

    /** 요청을 없애므로 호출마다 준비한다. */
    @State(Scope.Benchmark)
    public static class ForCancel extends Pending {
        @Setup(Level.Invocation)
        public void prepare() {
            build();
        }
    }

    @Benchmark
    public void requestStateOfSampledTickets(ForRead state, Blackhole blackhole) {
        for (long ticket : state.sampled) {
            blackhole.consume(state.endpoint.requestState(ticket));
        }
    }

    @Benchmark
    public void cancelSampledTickets(ForCancel state) {
        for (long ticket : state.sampled) {
            state.endpoint.cancelRequest(ticket);
        }
    }
}
