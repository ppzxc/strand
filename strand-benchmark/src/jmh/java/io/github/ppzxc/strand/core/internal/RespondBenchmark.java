package io.github.ppzxc.strand.core.internal;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * 서버 {@code Endpoint}가 받아 둔 요청 {@code pending}개 모두에 {@code respond}하는 비용(#423). 응답 생성과 쓰기 대기열 비용이 대기 수와 무관한지
 * 본다. 대기 수가 100배(2000→200000)가 되면 시간도 100배 안팎(선형)이어야 한다.
 *
 * <p>호출마다 새로 준비하므로({@code Level.Invocation}) 준비는 시간에 들어가지 않는다. 호출이 끝나면 응답한 수만큼 송신이 쌓였는지 확인한다.
 */
@State(Scope.Benchmark)
public class RespondBenchmark {

    private static final byte[] BODY = new byte[16];

    /** 대기 요청 수. */
    @Param({"2000", "20000", "200000"})
    public int pending;

    @Param({"OLDEST_FIRST", "NEWEST_FIRST"})
    public ResponseOrder order;

    private Endpoint server;
    private long[] streamIds;

    @Setup(Level.Invocation)
    public void prepare() {
        server = BenchSupport.establishWithPendingRequests(pending)[1];
        streamIds = BenchSupport.receivedStreamIds(server, order);
        BenchSupport.expectCount("받은 요청", pending, streamIds.length);
    }

    @Benchmark
    public void respondToAll() {
        for (long streamId : streamIds) {
            server.respond(streamId, 0, BODY);
        }
    }

    @TearDown(Level.Invocation)
    public void verify() {
        BenchSupport.expectCount(
                "응답 송신", pending, BenchSupport.drainMessages(server).size());
    }
}
