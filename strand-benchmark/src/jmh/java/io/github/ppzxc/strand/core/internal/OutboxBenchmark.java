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
 * 쓰기 대기열(outbox)과 번호표 조회의 비용 증가율(#200, #202). 요청 수가 10배가 되면 시간도 10배 안팎(선형)이어야 한다.
 *
 * <p>모든 벤치마크가 같은 상태에서 시작한다. 서버가 상대 요청 n개를 받아 두었고(이벤트는 비우지 않는다), 서버 자신의 쓰기 전 요청 n개(기한
 * 있음)가 쌓여 있다. 이 준비는 {@code Level.Invocation}이라 시간에 들어가지 않는다(측정 대상이 밀리초 이상이다).
 *
 * <ul>
 *   <li>{@link #closeWithQueuedRequests}: 오류로 닫는다. 받아 둔 요청마다 RESET을 돌려보낸다(#200).
 *   <li>{@link #cancelAllByTicket}: 쓰기 전 요청 n개를 낸 순서대로 번호표로 모두 취소한다(#200의 outbox 제거). 번호표 탐색은 매번 맨 앞에서
 *       맞아 최선의 경우다.
 *   <li>{@link #cancelAllByTicketNewestFirst}: 최근 번호표부터 모두 취소한다. 번호표 탐색이 최악이다(#202).
 *   <li>{@link #requestStateOfEveryTicket}: 쓰기 전 요청 n개의 상태를 번호표마다 묻는다. 한 번이 O(n)이면 전체가 이차다(#202).
 *   <li>{@link #advanceOncePerPendingRequest}: 시계를 n번 민다. 한 번이 대기 요청 전체를 훑으면 전체가 이차다(#202).
 * </ul>
 */
@State(Scope.Benchmark)
public class OutboxBenchmark {

    private static final byte[] EMPTY = new byte[0];
    private static final long DEADLINE_MS = 1_000_000;

    /** 요청 수. */
    @Param({"2000", "20000"})
    public int requests;

    private Endpoint server;
    private long[] tickets;

    @Setup(Level.Invocation)
    public void prepare() {
        Endpoint[] pair = BenchSupport.establish(EndpointConfig.defaults().maxConcurrentStreams(requests));
        Endpoint client = pair[0];
        server = pair[1];
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
        return server.drainMessages();
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
            server.advance(1); // advance는 증분이다. 기한(DEADLINE_MS)은 넘지 않는다.
        }
    }
}
