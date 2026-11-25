package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.api.Request;
import io.github.ppzxc.strand.api.RequestHandle;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.nio.Listener;
import io.github.ppzxc.strand.nio.NioStrandClient;
import io.github.ppzxc.strand.nio.NioStrandConnection;
import io.github.ppzxc.strand.nio.NioStrandServer;
import io.github.ppzxc.strand.nio.PassageWrapped;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class PartialWriteObservation {

    private static final long WRITE_WAIT_SECONDS = 5;
    private static final long RESUME_WRITE_CAPACITY = 1_000_000;

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

    public static Result cancelAfterPartialWrite(int type, byte[] payload, int initialWriteCapacity) throws Exception {
        ExecutorService executor = TestNio.newExecutor("partial-write-observer");
        BlockingQueue<ControlledPassage> clientPassages = new LinkedBlockingQueue<>();
        BlockingQueue<ControlledPassage> serverPassages = new LinkedBlockingQueue<>();
        PassageWrapping.Wrapper wrapper = (side, passage) -> {
            ControlledPassage controlled = new ControlledPassage(passage);
            (side == PassageWrapping.Side.CLIENT ? clientPassages : serverPassages).add(controlled);
            return controlled;
        };
        NioStrandServer server = PassageWrapped.server(TestNio.serverBuilder(executor), wrapper)
                .handler(request -> {})
                .build();
        NioStrandClient client =
                PassageWrapped.client(TestNio.clientBuilder(executor), wrapper).build();
        try {
            Listener listener = server.listen(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
                    .toCompletableFuture()
                    .get(WRITE_WAIT_SECONDS, TimeUnit.SECONDS);
            NioStrandConnection connection = client.connect(new InetSocketAddress(
                            InetAddress.getLoopbackAddress(), ((InetSocketAddress) listener.localAddress()).getPort()))
                    .toCompletableFuture()
                    .get(WRITE_WAIT_SECONDS, TimeUnit.SECONDS);
            ControlledPassage clientPassage = clientPassages.poll(WRITE_WAIT_SECONDS, TimeUnit.SECONDS);
            if (clientPassage == null) {
                throw new AssertionError("client passage was not wrapped");
            }

            int firstWrittenFrame = clientPassage.writtenFrames().size();
            clientPassage.writeCapacity(initialWriteCapacity);
            RequestHandle handle = connection.request(Request.of(type, payload));
            AtomicInteger resultDeliveries = new AtomicInteger();
            CompletableFuture<Void> deliveryObserved = new CompletableFuture<>();
            handle.result().observe(ignored -> {
                resultDeliveries.incrementAndGet();
                deliveryObserved.complete(null);
            });
            if (!clientPassage.awaitBlockedWrites(1)) {
                throw new AssertionError("NIO writer did not block after the partial request write");
            }
            handle.cancel();
            deliveryObserved.get(WRITE_WAIT_SECONDS, TimeUnit.SECONDS);
            RequestResult result = handle.result().resultNow();
            clientPassage.addWriteCapacity(RESUME_WRITE_CAPACITY);
            awaitFrames(clientPassage, firstWrittenFrame + 2);

            ControlledPassage serverPassage = serverPassages.poll(WRITE_WAIT_SECONDS, TimeUnit.SECONDS);
            if (serverPassage == null || !awaitRead(serverPassage, type)) {
                throw new AssertionError("peer did not receive the completed request frame");
            }
            if (resultDeliveries.get() != 1) {
                throw new AssertionError("request result was delivered " + resultDeliveries.get() + " times");
            }
            List<byte[]> frames = new ArrayList<>();
            List<ControlledPassage.Frame> written = clientPassage.writtenFrames();
            for (ControlledPassage.Frame frame : written.subList(firstWrittenFrame, written.size())) {
                frames.add(frame.bytes());
            }
            return new Result(
                    result.status(), Certainty.valueOf(result.certainty().name()), resultDeliveries.get(), frames);
        } finally {
            client.shutdown();
            server.shutdown();
            executor.shutdownNow();
        }
    }

    private static boolean awaitRead(ControlledPassage passage, int type) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(WRITE_WAIT_SECONDS);
        while (System.nanoTime() < limit) {
            for (ControlledPassage.Frame frame : passage.readFrames()) {
                if (frame.type() == type) {
                    return true;
                }
            }
            Thread.sleep(2);
        }
        return false;
    }

    private static void awaitFrames(ControlledPassage passage, int count) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(WRITE_WAIT_SECONDS);
        while (passage.writtenFrames().size() < count) {
            if (System.nanoTime() > limit) {
                throw new AssertionError("completed request frame and RESET were not both written");
            }
            Thread.sleep(2);
        }
    }
}
