package io.github.ppzxc.strand.nio;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 앱이 관찰하는 종료 관련 Stage의 완료 경로. 평소처럼 앱 실행기에 완료 작업을 제출하되, 실행기가 작업을 받고 {@code fallbackNanos} 안에 실행하지 않으면 감독
 * 스레드가 직접 완료한다. 실행기가 제출을 거절하면 호출 스레드에서 곧바로 완료한다. 두 경로 중 먼저 도달한 쪽만 완료 작업을 실행한다.
 */
final class StageCompletion {
    private static final class Supervisor {
        static final ScheduledThreadPoolExecutor INSTANCE = create();

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

    /**
     * 완료 작업을 한 번만 실행되게 앱 실행기와 기한 경로로 제출한다.
     *
     * @param executor 앱 실행기
     * @param completion 완료 작업
     * @param fallbackNanos 실행기가 실행하지 않을 때 직접 완료하기까지 기다리는 시간
     */
    static void complete(Executor executor, Runnable completion, long fallbackNanos) {
        AtomicBoolean claimed = new AtomicBoolean();
        Runnable once = () -> {
            if (claimed.compareAndSet(false, true)) {
                completion.run();
            }
        };
        ScheduledFuture<?> fallback = Supervisor.INSTANCE.schedule(once, fallbackNanos, TimeUnit.NANOSECONDS);
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
