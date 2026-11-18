package io.github.ppzxc.strand.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Validated request wire extensions. */
public final class Extensions {
    private final Map<Integer, byte[]> values;

    private Extensions(Map<Integer, byte[]> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<byte[]> get(int tag) {
        byte[] value = values.get(tag);
        return value == null ? Optional.empty() : Optional.of(value.clone());
    }

    public static final class Builder {
        private final Map<Integer, byte[]> values = new HashMap<>();

        public Builder timeoutMillis(long millis) {
            if (millis < 0 || millis > 0xffffffffL) throw new IllegalArgumentException("timeout out of range");
            return put(
                    1,
                    new byte[] {(byte) (millis >>> 24), (byte) (millis >>> 16), (byte) (millis >>> 8), (byte) millis});
        }

        public Builder idempotencyKey(byte[] key) {
            Objects.requireNonNull(key);
            if (key.length < 1 || key.length > 64) throw new IllegalArgumentException("invalid key length");
            return put(3, key);
        }

        public Builder put(int tag, byte[] value) {
            Objects.requireNonNull(value);
            if (tag < 0 || tag > 0xffff || value.length > 8188) throw new IllegalArgumentException("invalid extension");
            if ((tag == 1 && value.length != 4) || (tag == 3 && (value.length == 0 || value.length > 64)))
                throw new IllegalArgumentException("invalid known extension");
            values.put(tag, value.clone());
            return this;
        }

        public Extensions build() {
            int size = 0;
            Map<Integer, byte[]> copy = new HashMap<>();
            for (Map.Entry<Integer, byte[]> entry : values.entrySet()) {
                size += 4 + entry.getValue().length;
                if (size > 8192) throw new IllegalArgumentException("extensions too large");
                copy.put(entry.getKey(), entry.getValue().clone());
            }
            return new Extensions(copy);
        }
    }
}
