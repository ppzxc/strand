package io.github.ppzxc.strand.api;

import java.util.Map;
import java.util.Objects;

public final class ShutdownDeadlineExceededException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final ShutdownResult result;

    public ShutdownDeadlineExceededException(ShutdownResult result) {
        super("shutdown deadline exceeded");
        this.result = Objects.requireNonNull(result);
        if (result.kind() != ShutdownKind.DEADLINE_EXCEEDED)
            throw new IllegalArgumentException("result must be a deadline failure");
    }

    public Map<Resource, Long> remainingResources() {
        return result.remainingResources();
    }
}
