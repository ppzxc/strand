package io.github.ppzxc.strand.api;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/** Limits applying to each connection. */
public final class ConnectionLimits {
    private final Map<Resource, Long> values;

    private ConnectionLimits(Map<Resource, Long> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public long get(Resource resource) {
        return Objects.requireNonNull(values.get(resource), "resource is not connection-scoped");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends LimitBuilder<Builder> {
        @Override
        Builder self() {
            return this;
        }

        public ConnectionLimits build() {
            return new ConnectionLimits(finish(14));
        }
    }
}
