package io.github.ppzxc.strand.api;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/** Shutdown outcome and snapshot of resources still outstanding at the deadline. */
public final class ShutdownResult implements java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private final ShutdownKind kind;

    @SuppressWarnings("serial")
    private final Map<Resource, Long> remaining;

    private ShutdownResult(ShutdownKind kind, Map<Resource, Long> remaining) {
        this.kind = kind;
        EnumMap<Resource, Long> snapshot = new EnumMap<>(Resource.class);
        snapshot.putAll(remaining);
        this.remaining = Collections.unmodifiableMap(snapshot);
    }

    public static ShutdownResult completed() {
        return new ShutdownResult(ShutdownKind.COMPLETED, Collections.emptyMap());
    }

    public static ShutdownResult deadlineExceeded(Map<Resource, Long> remaining) {
        return new ShutdownResult(ShutdownKind.DEADLINE_EXCEEDED, remaining);
    }

    public ShutdownKind kind() {
        return kind;
    }

    public Map<Resource, Long> remainingResources() {
        return remaining;
    }
}
