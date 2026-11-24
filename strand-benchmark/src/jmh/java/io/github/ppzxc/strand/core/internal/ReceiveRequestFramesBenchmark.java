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
 * 실제 요청 프레임 {@value #REQUESTS}개를 서버 {@code Endpoint}가 받는 비용(#423). 프레임 파싱과 {@code REQUEST_RECEIVED} 이벤트 생성이
 * 들어온다. 본문이 커질 때(64B, 1KiB, 64KiB) 시간이 본문 크기에 선형이어야 한다. 무시되는 예약 프레임을 먹이는
 * {@link ReceiveManySmallFramesBenchmark}·{@link ReceiveBigFrameBenchmark}는 대조군이다.
 *
 * <p>요청 프레임은 클라이언트 {@code Endpoint}가 만든 것이고 스트림 번호가 한 번만 쓸 수 있어서 호출마다 새로 준비한다({@code Level.Invocation},
 * 시간 밖). 호출이 끝나면 요청 수만큼 이벤트가 나왔는지 확인한다.
 */
@State(Scope.Benchmark)
public class ReceiveRequestFramesBenchmark {

    private static final int REQUESTS = 1024;

    /** 요청 본문 크기(바이트). */
    @Param({"64", "1024", "65536"})
    public int bodyBytes;

    private Endpoint server;
    private List<OutboundMessage> frames;

    @Setup(Level.Invocation)
    public void prepare() {
        EndpointConfig config =
                EndpointConfig.defaults().maxConcurrentStreams(REQUESTS).maxFrameSize(1_048_576);
        Endpoint[] pair = BenchSupport.establish(config);
        server = pair[1];
        byte[] body = new byte[bodyBytes];
        for (int i = 0; i < REQUESTS; i++) {
            pair[0].request(BenchSupport.REQUEST_TYPE, body);
        }
        frames = BenchSupport.drainMessages(pair[0]);
        BenchSupport.expectCount("요청 프레임", REQUESTS, frames.size());
    }

    @Benchmark
    public void receive() {
        BenchSupport.receiveAll(server, frames);
    }

    @TearDown(Level.Invocation)
    public void verify() {
        BenchSupport.expectCount(
                "REQUEST_RECEIVED 이벤트", REQUESTS, BenchSupport.countEvents(server, Event.Kind.REQUEST_RECEIVED));
    }
}
