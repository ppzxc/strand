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

    /**
     * Validate transport-specific minimums once the frame and TLS configuration is known. The caller (a transport
     * builder: NIO in #316, Netty in #322) computes the result-event and TLS minimums; this method never invents them.
     */
    public void requireProgress(
            long frame, long control, long result, long plaintext, long ciphertext, long tlsControl) {
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
            if (minimum[i] <= 0 || get(kinds[i]) < minimum[i]) {
                throw new IllegalArgumentException("insufficient progress space: " + kinds[i]);
            }
        }
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
