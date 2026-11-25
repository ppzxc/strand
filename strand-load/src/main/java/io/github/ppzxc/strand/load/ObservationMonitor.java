package io.github.ppzxc.strand.load;

import io.github.ppzxc.strand.api.Observation;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class ObservationMonitor {
    private ObservationMonitor() {}

    static void watch(
            Observation observation,
            ScheduledExecutorService scheduler,
            AtomicBoolean completed,
            Runnable onFailure) {
        if (completed.get()) return;
        if (observation.isDone()) {
            if (observation.failureNow().isPresent() && completed.compareAndSet(false, true)) {
                onFailure.run();
            }
            return;
        }
        scheduler.schedule(() -> watch(observation, scheduler, completed, onFailure), 1, TimeUnit.MILLISECONDS);
    }
}
