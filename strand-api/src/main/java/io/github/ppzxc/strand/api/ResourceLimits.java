package io.github.ppzxc.strand.api;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/** Connection and owner resource budgets. */
public final class ResourceLimits {
    private final ConnectionLimits connection;
    private final OwnerLimits owner;

    private ResourceLimits(ConnectionLimits connection, OwnerLimits owner) {
        this.connection = connection;
        this.owner = owner;
    }

    public ConnectionLimits perConnection() {
        return connection;
    }

    public OwnerLimits perOwner() {
        return owner;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private @Nullable ConnectionLimits connection;
        private @Nullable OwnerLimits owner;

        public Builder perConnection(ConnectionLimits value) {
            connection = Objects.requireNonNull(value);
            return this;
        }

        public Builder perOwner(OwnerLimits value) {
            owner = Objects.requireNonNull(value);
            return this;
        }

        public ResourceLimits build() {
            if (connection == null || owner == null) throw new IllegalStateException("both scopes are required");
            Resource[] progress = {
                Resource.FRAME_ASSEMBLY_BYTES,
                Resource.CONTROL_OUTPUT_BYTES,
                Resource.RESULT_EVENT_BYTES,
                Resource.TLS_PLAINTEXT_BYTES,
                Resource.TLS_CIPHERTEXT_BYTES,
                Resource.TLS_CONTROL_BYTES
            };
            for (Resource resource : progress) {
                if (owner.get(resource) < connection.get(resource)) {
                    throw new IllegalArgumentException(
                            "owner progress space is smaller than one connection: " + resource);
                }
            }
            return new ResourceLimits(connection, owner);
        }
    }
}
