package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** 수동 시계와 메모리 통로로 연결 드라이버 쌍을 실행하는 테스트 도구다. */
public final class InMemoryConnectionPair {

    /** 한 연결 끝의 드라이버와 관찰 가능한 상태. */
    public static final class Side {
        public final AtomicLong clock = new AtomicLong(1_000_000);
        public final InMemoryPassage passage;
        public final ConcurrentLinkedQueue<ConnectionClosed> closed = new ConcurrentLinkedQueue<>();
        public final List<IncomingRequest> received = new CopyOnWriteArrayList<>();
        public ConnectionDriver driver;

        private Side(InMemoryPassage passage) {
            this.passage = passage.manualTime();
        }

        public void tick(long ms) {
            clock.addAndGet(ms);
            passage.wakeup();
        }

        public void blockWrites() {
            passage.writeCapacity(0);
        }

        public void unblockWrites(long bytes) {
            passage.addWriteCapacity(bytes);
        }
    }

    private InMemoryConnectionPair() {}

    /** 핸드셰이크를 마친 연결 한 쌍. */
    public static Side[] connected(EndpointConfig config, ExecutorService handlers, boolean messages) {
        InMemoryPassage.Pair link = messages ? InMemoryPassage.messages() : InMemoryPassage.streams();
        Side client = new Side(link.first());
        Side server = new Side(link.second());
        start(server, false, config, handlers);
        start(client, true, config, handlers);
        settle(handlers, client, server);
        return new Side[] {client, server};
    }

    /** 전송 스레드와 요청 처리기가 가라앉을 때까지 기다린다. */
    public static void settle(ExecutorService handlers, Side... sides) {
        InMemoryPassage[] passages = new InMemoryPassage[sides.length];
        for (int i = 0; i < sides.length; i++) passages[i] = sides[i].passage;
        InMemoryPassage.awaitQuiescence(passages);
        try {
            handlers.submit(() -> {}).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("handler executor did not settle", e);
        }
    }

    private static void start(Side side, boolean client, EndpointConfig config, ExecutorService handlers) {
        ConnectionListener listener = new ConnectionListener() {
            @Override
            public void onClosed(ConnectionClosed event) {
                side.closed.add(event);
            }
        };
        ConnectionDriver.Builder builder =
                client ? ConnectionDriver.client(config, side.passage) : ConnectionDriver.server(config, side.passage);
        side.driver = builder.handler(handlers, side.received::add)
                .listener(listener)
                .clock(side.clock::get)
                .start();
    }
}
