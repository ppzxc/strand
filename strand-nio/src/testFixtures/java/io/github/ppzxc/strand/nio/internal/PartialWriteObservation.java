package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** 실행 경로별 적합성 테스트가 전송 계층의 부분 쓰기를 관찰할 수 있게 한다. */
public final class PartialWriteObservation {

    private static final long ACK_TIMEOUT_MILLISECONDS = 1_000;
    private static final long IDLE_INTERVAL_MILLISECONDS = 3_000;
    private static final long WRITE_WAIT_SECONDS = 5;
    private static final int RESUME_WRITE_CAPACITY = 1_000_000;

    /** 실제 NIO 실행에서 관찰한 결과. */
    public static final class Result {
        private final int status;
        private final Certainty certainty;
        private final List<byte[]> frames;
        private final int resultDeliveries;

        private Result(int status, Certainty certainty, int resultDeliveries, List<byte[]> frames) {
            this.status = status;
            this.certainty = certainty;
            this.frames = Collections.unmodifiableList(new ArrayList<>(frames));
            this.resultDeliveries = resultDeliveries;
        }

        public int status() {
            return status;
        }

        public Certainty certainty() {
            return certainty;
        }

        public List<byte[]> frames() {
            return frames;
        }

        public int resultDeliveries() {
            return resultDeliveries;
        }
    }

    private PartialWriteObservation() {}

    /** 요청 프레임을 제한된 바이트 수만 쓴 뒤 취소하고, 쓰기를 풀어 완성 프레임을 돌려준다. */
    public static Result cancelAfterPartialWrite(int type, byte[] payload, int initialWriteCapacity) throws Exception {
        ExecutorService handlers = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "partial-write-observer");
            thread.setDaemon(true);
            return thread;
        });
        EndpointConfig config = EndpointConfig.defaults()
                .ackTimeoutMs(ACK_TIMEOUT_MILLISECONDS)
                .idleIntervalMs(IDLE_INTERVAL_MILLISECONDS);
        InMemoryConnectionPair.Side[] sides = null;
        try {
            sides = InMemoryConnectionPair.connected(config, handlers, false);
            InMemoryConnectionPair.Side clientSide = sides[0];
            InMemoryConnectionPair.Side serverSide = sides[1];
            InMemoryPassage clientPassage = clientSide.passage;

            int firstWrittenFrame = clientPassage.written().size();
            clientPassage.writeCapacity(initialWriteCapacity);
            RequestHandle handle = clientSide.driver.request(type, payload);
            AtomicInteger resultDeliveries = new AtomicInteger();
            CompletableFuture<Void> deliveryObserved =
                    handle.result().thenAccept(ignored -> resultDeliveries.incrementAndGet());
            if (!clientSide.driver.awaitWriteBlocked(true, WRITE_WAIT_SECONDS, TimeUnit.SECONDS)) {
                throw new AssertionError("NIO writer did not block after the partial request write");
            }
            handle.cancel();
            RequestResult result = handle.result().get(WRITE_WAIT_SECONDS, TimeUnit.SECONDS);
            deliveryObserved.get(WRITE_WAIT_SECONDS, TimeUnit.SECONDS);
            InMemoryConnectionPair.settle(handlers, clientSide, serverSide);
            clientPassage.addWriteCapacity(RESUME_WRITE_CAPACITY);
            InMemoryConnectionPair.settle(handlers, clientSide, serverSide);
            if (resultDeliveries.get() != 1) {
                throw new AssertionError("request result was delivered " + resultDeliveries.get() + " times");
            }
            if (serverSide.received.size() != 1) {
                throw new AssertionError("peer did not receive the completed request frame");
            }
            return new Result(
                    result.status(),
                    result.certainty(),
                    resultDeliveries.get(),
                    clientPassage
                            .written()
                            .subList(firstWrittenFrame, clientPassage.written().size()));
        } finally {
            if (sides != null) {
                sides[0].driver.close();
                sides[1].driver.close();
            }
            handlers.shutdownNow();
        }
    }
}
