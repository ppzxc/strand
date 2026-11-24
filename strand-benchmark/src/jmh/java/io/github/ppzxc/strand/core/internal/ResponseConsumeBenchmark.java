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
 * 클라이언트 {@code Endpoint}가 응답 {@code pending}개를 받아 내 요청 장부에서 소비하는 비용(#423, #207의 응답 쪽). 번호표 조회가 대기 수와
 * 무관한지 본다. 대기 수가 100배(2000→200000)가 되면 시간도 100배 안팎(선형)이어야 한다.
 *
 * <p>서버가 요청 순서의 정방향 또는 역방향으로 응답한 프레임을 미리 만들어 두고({@code Level.Invocation}, 시간 밖) 클라이언트에 넣는다. 호출이 끝나면
 * 응답한 수만큼 {@code RESULT} 이벤트가 나왔는지 확인한다.
 */
@State(Scope.Benchmark)
public class ResponseConsumeBenchmark {

    private static final byte[] BODY = new byte[16];

    /** 대기 요청 수. */
    @Param({"2000", "20000", "200000"})
    public int pending;

    @Param({"OLDEST_FIRST", "NEWEST_FIRST"})
    public ResponseOrder order;

    private Endpoint client;
    private List<OutboundMessage> responses;

    @Setup(Level.Invocation)
    public void prepare() {
        Endpoint[] pair = BenchSupport.establishWithPendingRequests(pending);
        client = pair[0];
        Endpoint server = pair[1];
        BenchSupport.drainEvents(client);
        for (long streamId : BenchSupport.receivedStreamIds(server, order)) {
            server.respond(streamId, 0, BODY);
        }
        responses = BenchSupport.drainMessages(server);
        if (responses.size() != pending) {
            throw new IllegalStateException("응답 프레임이 " + pending + "개가 아니라 " + responses.size() + "개다");
        }
    }

    @Benchmark
    public void consumeAll() {
        for (OutboundMessage response : responses) {
            BenchSupport.receive(client, response.bytes());
        }
    }

    @TearDown(Level.Invocation)
    public void verify() {
        long results = BenchSupport.drainEvents(client).stream()
                .filter(e -> e.kind() == Event.Kind.RESULT)
                .count();
        if (results != pending) {
            throw new IllegalStateException("RESULT 이벤트가 " + pending + "개가 아니라 " + results + "개다");
        }
    }
}
