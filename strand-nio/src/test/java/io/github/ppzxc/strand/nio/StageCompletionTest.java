package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class StageCompletionTest {
    private static final long FALLBACK_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    private static void awaitCount(AtomicInteger count, int expected) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (count.get() != expected) {
            if (System.nanoTime() > end) {
                throw new AssertionError("완료 횟수가 " + expected + "가 되지 않았다: " + count.get());
            }
            Thread.sleep(5);
        }
    }

    @Test
    void anExecutorTaskThatRunsAfterTheDeadlinePathAlreadyCompletedDoesNotCompleteAgain() throws Exception {
        List<Runnable> held = new CopyOnWriteArrayList<>();
        Executor holding = held::add;
        AtomicInteger completions = new AtomicInteger();

        StageCompletion.complete(holding, completions::incrementAndGet, FALLBACK_NANOS);
        awaitCount(completions, 1);
        held.get(0).run();

        assertThat(completions).hasValue(1);
    }

    @Test
    void anExecutorThatRunsTheTaskInTimeCompletesOnceAndTheDeadlinePathNeverFires() throws Exception {
        AtomicInteger completions = new AtomicInteger();

        StageCompletion.complete(Runnable::run, completions::incrementAndGet, FALLBACK_NANOS);
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(FALLBACK_NANOS) * 4);

        assertThat(completions).hasValue(1);
    }

    @Test
    void aRejectingExecutorCompletesOnTheCallerOnceAndTheDeadlinePathNeverFires() throws Exception {
        AtomicInteger completions = new AtomicInteger();
        Executor rejecting = task -> {
            throw new java.util.concurrent.RejectedExecutionException();
        };

        StageCompletion.complete(rejecting, completions::incrementAndGet, FALLBACK_NANOS);
        assertThat(completions).hasValue(1);
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(FALLBACK_NANOS) * 4);

        assertThat(completions).hasValue(1);
    }

    @Test
    void aBlockedFallbackCallbackDoesNotBlockAnotherFallback() throws Exception {
        Executor holding = task -> {};
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondCompleted = new CountDownLatch(1);

        StageCompletion.complete(holding, () -> {
            firstStarted.countDown();
            try {
                releaseFirst.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, FALLBACK_NANOS);
        assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();

        StageCompletion.complete(holding, secondCompleted::countDown, FALLBACK_NANOS);
        assertThat(secondCompleted.await(5, TimeUnit.SECONDS)).isTrue();
        releaseFirst.countDown();
    }
}
