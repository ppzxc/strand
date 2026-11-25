package io.github.ppzxc.strand.api;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

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

    public void requireProgress(
            long frame, long control, long result, long plaintext, long ciphertext, long tlsControl) {
        connection.requireProgress(frame, control, result, plaintext, ciphertext, tlsControl);
        Resource[] kinds = {
            Resource.FRAME_ASSEMBLY_BYTES,
            Resource.CONTROL_OUTPUT_BYTES,
            Resource.RESULT_EVENT_BYTES,
            Resource.TLS_PLAINTEXT_BYTES,
            Resource.TLS_CIPHERTEXT_BYTES,
            Resource.TLS_CONTROL_BYTES
        };
        long[] minimum = {frame, control, result, plaintext, ciphertext, tlsControl};
        for (int i = 0; i < kinds.length; i++) {
            if (owner.get(kinds[i]) < minimum[i]) {
                throw new IllegalArgumentException("insufficient progress space: " + kinds[i]
                        + " scope=per-owner required=" + minimum[i] + " configured=" + owner.get(kinds[i]));
            }
        }
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
            return new ResourceLimits(connection, owner);
        }
    }
}
