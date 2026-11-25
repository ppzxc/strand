package io.github.ppzxc.strand.integration.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.Event;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class CoreWithdrawalRaceTest {
    private static final int ITERATIONS = 60;
    private static final int FRAME_PAYLOAD = 256 * 1024;

    private static void awaitIdle(java.util.function.BooleanSupplier pending) throws InterruptedException {
        for (int i = 0; i < 5000 && pending.getAsBoolean(); i++) Thread.sleep(2);
        assertThat(pending.getAsBoolean()).as("출력이 모두 처리됨").isFalse();
    }

    private static void spin(long nanos) {
        long end = System.nanoTime() + nanos;
        while (System.nanoTime() < end) {}
    }

    private static void issueAndCancel(
            int iteration,
            java.util.function.Consumer<Runnable> command,
            java.util.function.Consumer<Runnable> enqueue,
            Runnable enqueuePump,
            java.util.function.BooleanSupplier pending,
            io.github.ppzxc.strand.core.internal.Endpoint endpoint,
            byte[] payload)
            throws InterruptedException {
        AtomicLong ticket = new AtomicLong();
        switch (iteration % 4) {
            case 0:
                command.accept(() -> ticket.set(endpoint.request(OpenEndpoint.REQUEST_TYPE, payload)));
                spin(ThreadLocalRandom.current().nextLong(0, TimeUnit.MICROSECONDS.toNanos(300)));
                command.accept(() -> endpoint.cancelRequest(ticket.get()));
                break;
            case 1:
                enqueue.accept(() -> ticket.set(endpoint.request(OpenEndpoint.REQUEST_TYPE, payload)));
                enqueue.accept(() -> endpoint.cancelRequest(ticket.get()));
                enqueuePump.run();
                break;
            case 3:
                command.accept(() -> ticket.set(endpoint.request(OpenEndpoint.REQUEST_TYPE, payload)));
                awaitIdle(pending);
                command.accept(() -> endpoint.cancelRequest(ticket.get()));
                break;
            default:
                command.accept(() -> {
                    ticket.set(endpoint.request(OpenEndpoint.REQUEST_TYPE, payload));
                    endpoint.cancelRequest(ticket.get());
                });
                break;
        }
    }

    private static int[] verify(byte[] wire, List<Event> events) {
        List<Event> results = events.stream()
                .filter(e -> e.kind() == Event.Kind.RESULT)
                .collect(java.util.stream.Collectors.toList());
        assertThat(results).hasSize(1);
        Certainty certainty = results.get(0).certainty().get();
        List<byte[]> frames = OpenEndpoint.frames(wire);
        if (frames.isEmpty()) {
            assertThat(certainty).isEqualTo(Certainty.NOT_PROCESSED);
            return new int[] {1, 0};
        }
        assertThat(frames).hasSize(2);
        assertThat(OpenEndpoint.isComplete(frames.get(0))).isTrue();
        assertThat(OpenEndpoint.typeOf(frames.get(0))).isEqualTo(OpenEndpoint.REQUEST_TYPE);
        assertThat(frames.get(0)).hasSize(4 + 16 + FRAME_PAYLOAD);
        assertThat(OpenEndpoint.isComplete(frames.get(1))).isTrue();
        assertThat(OpenEndpoint.typeOf(frames.get(1))).isEqualTo(OpenEndpoint.FRAME_TYPE_RESET);
        assertThat(certainty).isEqualTo(Certainty.UNKNOWN);
        return new int[] {0, 1};
    }

    @Test
    void tcpCancellationRacingTheSocketWriteNeverLeavesAPartialFrameOrOutOfOrderReset() throws Exception {
        ExecutorService reader = Executors.newCachedThreadPool();
        int withdrawn = 0;
        int delivered = 0;
        try {
            for (int i = 0; i < ITERATIONS; i++) {
                try (TcpFixture fixture = new TcpFixture()) {
                    CompletableFuture<byte[]> wire = CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return fixture.readToEnd();
                                } catch (java.io.IOException e) {
                                    throw new java.io.UncheckedIOException(e);
                                }
                            },
                            reader);
                    byte[] payload = new byte[FRAME_PAYLOAD];
                    Arrays.fill(payload, (byte) 3);
                    java.util.function.BooleanSupplier pending = () -> {
                        try {
                            return fixture.onLoop(fixture.driver::hasPendingOutput);
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    };
                    issueAndCancel(
                            i,
                            fixture.driver::command,
                            fixture.driver::enqueue,
                            fixture.driver::enqueuePump,
                            pending,
                            fixture.endpoint,
                            payload);
                    awaitIdle(pending);
                    List<Event> events = fixture.onLoop(() -> fixture.endpoint.drainEvents(Integer.MAX_VALUE));
                    fixture.closeAndSettle();
                    int[] outcome = verify(wire.get(10, TimeUnit.SECONDS), events);
                    assertThat(fixture.onLoop(fixture.driver::allReleased)).isTrue();
                    withdrawn += outcome[0];
                    delivered += outcome[1];
                }
            }
        } finally {
            reader.shutdownNow();
        }
        System.out.println("tcp race outcomes: withdrawn=" + withdrawn + " deliveredThenReset=" + delivered);
        assertThat(withdrawn).as("쓰기 전에 취소가 처리된 순서").isPositive();
        assertThat(delivered).as("프레임이 먼저 전달된 순서").isPositive();
    }

    @Test
    void tlsCancellationRacingPlaintextConsumptionNeverLeavesAPartialFrameOrOutOfOrderReset() throws Exception {
        ExecutorService reader = Executors.newCachedThreadPool();
        int withdrawn = 0;
        int delivered = 0;
        try {
            for (int i = 0; i < ITERATIONS; i++) {
                try (TlsFixture fixture = new TlsFixture(i % 2 == 0 ? "TLSv1.2" : "TLSv1.3", 2)) {
                    CompletableFuture<byte[]> wire = CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return fixture.readToEnd();
                                } catch (java.io.IOException e) {
                                    throw new java.io.UncheckedIOException(e);
                                }
                            },
                            reader);
                    byte[] payload = new byte[FRAME_PAYLOAD];
                    Arrays.fill(payload, (byte) 3);
                    java.util.function.BooleanSupplier pending = () -> {
                        try {
                            return fixture.onLoop(fixture.driver::hasPendingOutput)
                                    || fixture.onLoop(fixture.driver::queuedCiphertextBytes) > 0;
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    };
                    issueAndCancel(
                            i,
                            fixture.driver::command,
                            fixture.driver::enqueue,
                            fixture.driver::enqueuePump,
                            pending,
                            fixture.endpoint,
                            payload);
                    awaitIdle(pending);
                    List<Event> events = fixture.onLoop(() -> fixture.endpoint.drainEvents(Integer.MAX_VALUE));
                    fixture.driver.closeGracefully();
                    int[] outcome = verify(wire.get(10, TimeUnit.SECONDS), events);
                    fixture.channel.closeFuture().sync();
                    fixture.onLoop(() -> null);
                    assertThat(fixture.onLoop(fixture.driver::allReleased)).isTrue();
                    withdrawn += outcome[0];
                    delivered += outcome[1];
                }
            }
        } finally {
            reader.shutdownNow();
        }
        System.out.println("tls race outcomes: withdrawn=" + withdrawn + " deliveredThenReset=" + delivered);
        assertThat(withdrawn).as("평문 소비 전에 취소가 처리된 순서").isPositive();
        assertThat(delivered).as("프레임이 먼저 전달된 순서").isPositive();
    }
}
