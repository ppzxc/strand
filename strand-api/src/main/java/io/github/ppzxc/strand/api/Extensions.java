package io.github.ppzxc.strand.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class Extensions {
    private final Map<Integer, byte[]> values;

    private Extensions(Map<Integer, byte[]> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public static Builder builder() {
        return new Builder();
    }

    public byte[] toWire() {
        java.util.TreeMap<Integer, byte[]> sorted = new java.util.TreeMap<>(values);
        int size = 0;
        for (byte[] value : sorted.values()) size += 4 + value.length;
        byte[] out = new byte[size];
        int at = 0;
        for (Map.Entry<Integer, byte[]> entry : sorted.entrySet()) {
            byte[] value = entry.getValue();
            out[at++] = (byte) (entry.getKey() >>> 8);
            out[at++] = (byte) (int) entry.getKey();
            out[at++] = (byte) (value.length >>> 8);
            out[at++] = (byte) value.length;
            System.arraycopy(value, 0, out, at, value.length);
            at += value.length;
        }
        return out;
    }

    public static Extensions fromWire(byte[] wire) {
        Objects.requireNonNull(wire, "wire");
        Builder builder = builder();
        Map<Integer, Boolean> seen = new HashMap<>();
        int at = 0;
        while (at < wire.length) {
            if (wire.length - at < 4) throw new IllegalArgumentException("truncated extension header");
            int tag = ((wire[at] & 0xff) << 8) | (wire[at + 1] & 0xff);
            int length = ((wire[at + 2] & 0xff) << 8) | (wire[at + 3] & 0xff);
            at += 4;
            if (length > wire.length - at) throw new IllegalArgumentException("truncated extension value");
            if (seen.put(tag, Boolean.TRUE) != null) throw new IllegalArgumentException("duplicate extension tag");
            builder.put(tag, java.util.Arrays.copyOfRange(wire, at, at + length));
            at += length;
        }
        return builder.build();
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
