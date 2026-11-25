package io.github.ppzxc.strand.core.internal;

import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class ReceiveRequestFramesBenchmark {

    private static final int REQUESTS = 1024;

    private static final long TARGET_BODY_BYTES = 16L * 1024 * 1024;

    @Param({"64", "1024", "65536"})
    public int bodyBytes;

    private EndpointConfig config;
    private List<OutboundMessage> frames;
    private Endpoint[] servers;

    int serverCount() {
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
        servers = new Endpoint[serverCount()];
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
        servers = null;
    }
}
