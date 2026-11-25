package io.github.ppzxc.strand.netty;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public final class UnreleasedHandler {
    private final PreparedHandler prepared;
    private final UnreleasedHandlers registry;
    private final Runnable releaseReservation;
    private final AtomicBoolean claimed = new AtomicBoolean();

    UnreleasedHandler(PreparedHandler prepared, UnreleasedHandlers registry, Runnable releaseReservation) {
        this.prepared = prepared;
        this.registry = registry;
        this.releaseReservation = releaseReservation;
    }

    public Optional<PreparedHandler> claim() {
        if (!claimed.compareAndSet(false, true)) {
            return Optional.empty();
        }
        registry.remove(this);
        releaseReservation.run();
        return Optional.of(prepared);
    }
}
