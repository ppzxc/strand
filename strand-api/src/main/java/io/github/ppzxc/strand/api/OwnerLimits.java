package io.github.ppzxc.strand.api;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class OwnerLimits {
    private final Map<Resource, Long> values;

    private OwnerLimits(Map<Resource, Long> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public long get(Resource resource) {
        return Objects.requireNonNull(values.get(resource));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder extends LimitBuilder<Builder> {
        @Override
        Builder self() {
            return this;
        }

        public Builder preparingConnections(long count) {
            set(Resource.PREPARING_CONNECTIONS, count);
            return this;
        }

        public Builder totalConnections(long count) {
            set(Resource.TOTAL_CONNECTIONS, count);
            return this;
        }

        public Builder preparationTasks(long resolve, long authentication, long factory) {
            set(Resource.RESOLVE_TASKS, resolve);
            set(Resource.AUTHENTICATION_TASKS, authentication);
            set(Resource.FACTORY_TASKS, factory);
            return this;
        }

        public OwnerLimits build() {
            return new OwnerLimits(finish(19));
        }
    }
}
