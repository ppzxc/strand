package io.github.ppzxc.strand.nio;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class StageCompletion {
    private static final class Supervisor {
        static final ScheduledThreadPoolExecutor INSTANCE = create();
        static final Executor COMPLETIONS = Executors.newCachedThreadPool(task -> {
            Thread thread = new Thread(task, "strand-nio-stage-completion");
            thread.setDaemon(true);
            return thread;
        });

        private static ScheduledThreadPoolExecutor create() {
            ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1, task -> {
                Thread thread = new Thread(task, "strand-nio-stage-supervisor");
                thread.setDaemon(true);
                return thread;
            });
            timer.setRemoveOnCancelPolicy(true);
            return timer;
        }
    }

    private StageCompletion() {}

    static void complete(Executor executor, Runnable completion, long fallbackNanos) {
        AtomicBoolean claimed = new AtomicBoolean();
        Runnable once = () -> {
            if (claimed.compareAndSet(false, true)) {
                completion.run();
            }
        };
        ScheduledFuture<?> fallback = Supervisor.INSTANCE.schedule(
                () -> Supervisor.COMPLETIONS.execute(once), fallbackNanos, TimeUnit.NANOSECONDS);
        try {
            executor.execute(() -> {
                fallback.cancel(false);
                once.run();
            });
        } catch (RuntimeException | Error e) {
            fallback.cancel(false);
            once.run();
        }
    }
}
