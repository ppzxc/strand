package io.github.ppzxc.strand.api;

import java.util.Optional;
import org.jspecify.annotations.Nullable;

public final class Observation {
    private boolean done;
    private @Nullable Throwable failure;

    synchronized void finish(@Nullable Throwable error) {
        failure = error;
        done = true;
    }

    public synchronized boolean isDone() {
        return done;
    }

    public synchronized Optional<Throwable> failureNow() {
        if (!done) {
            throw new IllegalStateException("observation is not complete");
        }
        return Optional.ofNullable(failure);
    }
}
