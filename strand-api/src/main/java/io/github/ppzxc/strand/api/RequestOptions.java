package io.github.ppzxc.strand.api;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/** Local options for a request. A deadline does not set a wire timeout extension. */
public final class RequestOptions {
    private final Optional<Duration> deadline;

    private RequestOptions(Optional<Duration> deadline) {
        this.deadline = deadline;
    }

    public static RequestOptions noDeadline() {
        return new RequestOptions(Optional.empty());
    }

    public static RequestOptions deadlineAfter(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        if (duration.isNegative()) {
            throw new IllegalArgumentException("deadline must not be negative");
        }
        return new RequestOptions(Optional.of(duration));
    }

    public Optional<Duration> deadline() {
        return deadline;
    }
}
