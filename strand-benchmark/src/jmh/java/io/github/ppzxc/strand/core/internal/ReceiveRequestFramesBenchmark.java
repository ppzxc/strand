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
 * <p>스트림 번호는 서버마다 한 번만 쓸 수 있어서 호출마다 새 서버를 준비한다({@code Level.Invocation}, 시간 밖). 요청 프레임은 번호가 같은 새 서버가
 * 그대로 받을 수 있으므로 프레임은 한 번만 만들고 서버만 새로 만든다. 64B 점은 서버 하나를 받는 데 약 0.15ms라 호출당 준비 대비 측정 시간이 작았다(#429).
 * 그래서 호출 하나가 서버 여러 대({@link #servers()})를 받아 호출당 측정 시간을 키운다. 서버 수는 서버 하나가 받는 요청 본문 총량이 {@value #TARGET_BODY_BYTES}바이트가
 * 되도록 정해서 64B는 256대, 1KiB는 16대, 64KiB는 1대다. 결과 시간은 호출 하나(서버 {@link #servers()}대)의 값이므로 서버 한 대 비용은 서버 수로 나눠 비교한다.
 * 호출이 끝나면 서버마다 요청 수만큼 이벤트가 나왔는지 확인한다.
 */
@State(Scope.Benchmark)
public class ReceiveRequestFramesBenchmark {

    private static final int REQUESTS = 1024;
    /** 호출 하나가 받는 요청 본문 총량(바이트). 서버 수 = 이 값 / (요청 수 × 본문 크기), 최소 1. */
    private static final long TARGET_BODY_BYTES = 16L * 1024 * 1024;

    /** 요청 본문 크기(바이트). */
    @Param({"64", "1024", "65536"})
    public int bodyBytes;

    private EndpointConfig config;
    private List<OutboundMessage> frames;
    private Endpoint[] servers;

    /** 호출 하나가 받는 서버 수. */
    int servers() {
        return (int) Math.max(1, TARGET_BODY_BYTES / ((long) REQUESTS * bodyBytes));
    }

    @Setup(Level.Trial)
    public void prepareFrames() {
        config = EndpointConfig.defaults().maxConcurrentStreams(REQUESTS).maxFrameSize(1_048_576);
        Endpoint client = BenchSupport.establish(config)[0];
        byte[] body = new byte[bodyBytes];
        for (int i = 0; i < REQUESTS; i++) {
            client.request(BenchSupport.REQUEST_TYPE, body);
        }
        frames = BenchSupport.drainMessages(client);
        BenchSupport.expectCount("요청 프레임", REQUESTS, frames.size());
    }

    @Setup(Level.Invocation)
    public void prepare() {
        servers = new Endpoint[servers()];
        for (int i = 0; i < servers.length; i++) {
            servers[i] = BenchSupport.establish(config)[1];
        }
    }

    @Benchmark
    public void receive() {
        for (Endpoint server : servers) {
            BenchSupport.receiveAll(server, frames);
        }
    }

    @TearDown(Level.Invocation)
    public void verify() {
        for (Endpoint server : servers) {
            BenchSupport.expectCount(
                    "REQUEST_RECEIVED 이벤트", REQUESTS, BenchSupport.countEvents(server, Event.Kind.REQUEST_RECEIVED));
        }
    }
}
