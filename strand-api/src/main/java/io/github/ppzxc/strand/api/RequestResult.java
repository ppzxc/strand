package io.github.ppzxc.strand.api;

import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/** Retained request status, certainty and application-owned payload. */
public final class RequestResult {
    private final int status;
    private final Certainty certainty;
    private final byte[] payload;
    private final @Nullable Resource resource;

    public RequestResult(int status, Certainty certainty, byte[] payload) {
        this(status, certainty, payload, null);
    }

    private RequestResult(int status, Certainty certainty, byte[] payload, @Nullable Resource resource) {
        if (status < 0 || status > 255) throw new IllegalArgumentException("invalid status");
        this.status = status;
        this.resource = resource;
        this.certainty = Objects.requireNonNull(certainty);
        this.payload = Objects.requireNonNull(payload).clone();
    }

    public static RequestResult resourceExhausted(Resource resource) {
        return new RequestResult(
                Status.RESOURCE_EXHAUSTED, Certainty.NOT_PROCESSED, new byte[0], Objects.requireNonNull(resource));
    }

    public Optional<Resource> resource() {
        return Optional.ofNullable(resource);
    }

    public int status() {
        return status;
    }

    public Certainty certainty() {
        return certainty;
    }

    public byte[] payload() {
        return payload;
    }
}
