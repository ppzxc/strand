package io.github.ppzxc.strand.netty.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SerialExecutorTest {

    @Test
    void runsTasksOneAtATimeInSubmissionOrder() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            SerialExecutor serial = new SerialExecutor(pool);
            AtomicInteger running = new AtomicInteger();
            AtomicInteger overlap = new AtomicInteger();
            List<Integer> order = java.util.Collections.synchronizedList(new ArrayList<>());
            CountDownLatch done = new CountDownLatch(50);
            for (int i = 0; i < 50; i++) {
                int n = i;
                serial.execute(() -> {
                    if (running.incrementAndGet() > 1) {
                        overlap.incrementAndGet();
                    }
                    order.add(n);
                    running.decrementAndGet();
                    done.countDown();
                });
            }
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(overlap.get()).isZero();
            for (int i = 0; i < 50; i++) {
                assertThat(order.get(i)).isEqualTo(i);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void doesNotRunTasksOnTheSubmittingThread() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            SerialExecutor serial = new SerialExecutor(pool);
            CountDownLatch done = new CountDownLatch(1);
            Thread[] ran = new Thread[1];
            serial.execute(() -> {
                ran[0] = Thread.currentThread();
                done.countDown();
            });
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(ran[0]).isNotSameAs(Thread.currentThread());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void rejectionOfTheBackingExecutorFailsTheSubmissionAndLeavesNothingQueued() {
        Executor rejecting = task -> {
            throw new RejectedExecutionException("full");
        };
        SerialExecutor serial = new SerialExecutor(rejecting);
        AtomicInteger ran = new AtomicInteger();

        assertThatThrownBy(() -> serial.execute(ran::incrementAndGet)).isInstanceOf(RejectedExecutionException.class);
        assertThat(serial.queuedTasks()).isZero();
        assertThat(ran).hasValue(0);
    }

    @Test
    void recoversAfterARejectionOnceTheBackingExecutorAcceptsAgain() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            boolean[] reject = {true};
            Executor flaky = task -> {
                if (reject[0]) {
                    throw new RejectedExecutionException("busy");
                }
                pool.execute(task);
            };
            SerialExecutor serial = new SerialExecutor(flaky);
            assertThatThrownBy(() -> serial.execute(() -> {})).isInstanceOf(RejectedExecutionException.class);
            reject[0] = false;
            CountDownLatch done = new CountDownLatch(1);

            serial.execute(done::countDown);

            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aThrowingTaskDoesNotStopLaterTasks() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            SerialExecutor serial = new SerialExecutor(pool);
            CountDownLatch done = new CountDownLatch(1);

            serial.execute(() -> {
                throw new IllegalStateException("boom");
            });
            serial.execute(done::countDown);

            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void rejectsDirectExecutors() {
        assertThatThrownBy(() -> SerialExecutor.requireNotDirect(Runnable::run))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsAsynchronousExecutors() {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            SerialExecutor.requireNotDirect(pool);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aBackingExecutorThatRunsTheDrainInlineRejectsWithoutRunningUserTasks() {
        SerialExecutor serial = new SerialExecutor(Runnable::run);
        AtomicInteger ran = new AtomicInteger();

        assertThatThrownBy(() -> serial.execute(ran::incrementAndGet))
                .isInstanceOf(RejectedExecutionException.class);

        assertThat(ran).hasValue(0);
        assertThat(serial.queuedTasks()).isZero();
    }
}
