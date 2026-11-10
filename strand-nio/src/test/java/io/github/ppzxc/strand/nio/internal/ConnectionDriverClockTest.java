package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 시계와 기한. 연결 드라이버에는 단조 시계를 주입하고, 통로는 시간 제한이 실제 시간으로 흐르지 않는 {@link InMemoryPassage#manualTime}이다. 테스트가
 * 시계를 올리고 통로를 깨운 뒤 양쪽이 가라앉기를 기다리므로 실제 시간을 기다리지 않는다. 메시지 통로를 써서 프레임 하나가 쓰기 하나가 되게 한다.
 */
class ConnectionDriverClockTest {

    private static final int APPLICATION_TYPE = 256;
    private static final int STATUS_DEADLINE_EXCEEDED = 4;

    /** 연결 드라이버와 그 시계·통로. */
    private static final class Side {
        final AtomicLong clock = new AtomicLong(1_000_000);
        final InMemoryPassage passage;
        ConnectionDriver driver;

        Side(InMemoryPassage passage) {
            this.passage = passage.manualTime();
        }

        /** 시계를 올리고 전송 스레드를 깨운다. */
        void tick(long ms) {
            clock.addAndGet(ms);
            passage.wakeup();
        }
    }

    private final List<ConnectionDriver> started = new ArrayList<>();
    private final ExecutorService handlers = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "test-handler");
        thread.setDaemon(true);
        return thread;
    });

    @AfterEach
    void stopAll() {
        started.forEach(ConnectionDriver::close);
        handlers.shutdownNow();
    }

    private static EndpointConfig config() {
        return EndpointConfig.defaults().webSocket();
    }

    private Side startClient(Side side, EndpointConfig config) {
        side.driver = ConnectionDriver.client(config, side.passage)
                .handler(handlers, request -> {})
                .clock(side.clock::get)
                .start();
        started.add(side.driver);
        return side;
    }

    private Side startServer(Side side, EndpointConfig config) {
        side.driver = ConnectionDriver.server(config, side.passage)
                .handler(handlers, request -> {})
                .clock(side.clock::get)
                .start();
        started.add(side.driver);
        return side;
    }

    private static void settle(Side first, Side second) {
        InMemoryPassage.awaitQuiescence(first.passage, second.passage);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void aRequestPastItsDeadlineEndsWithDeadlineExceededAndAnUnknownResultOnceWritten() {
        InMemoryPassage.Pair link = InMemoryPassage.messages();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        startServer(server, config());
        startClient(client, config());
        RequestHandle handle = client.driver.request(APPLICATION_TYPE, bytes("slow"), 100);
        settle(client, server);

        client.tick(100);
        settle(client, server);
        assertThat(handle.result()).isNotDone();

        client.tick(1);
        settle(client, server);
        assertThat(handle.result()).isDone();
        RequestResult result = handle.result().join();
        assertThat(result.status()).isEqualTo(STATUS_DEADLINE_EXCEEDED);
        assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
    }
}
