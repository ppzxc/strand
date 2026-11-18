package io.github.ppzxc.strand.api;

import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/** Retained local response admission decision. */
public final class ResponseAcceptance {
    private final AcceptanceKind kind;
    private final @Nullable Resource resource;

    private ResponseAcceptance(AcceptanceKind kind, @Nullable Resource resource) {
        this.kind = kind;
        this.resource = resource;
    }

    public static ResponseAcceptance accepted() {
        return new ResponseAcceptance(AcceptanceKind.ACCEPTED, null);
    }

    public static ResponseAcceptance alreadyResponded() {
        return new ResponseAcceptance(AcceptanceKind.ALREADY_RESPONDED, null);
    }

    public static ResponseAcceptance requestEnded() {
        return new ResponseAcceptance(AcceptanceKind.REQUEST_ENDED, null);
    }

    public static ResponseAcceptance resourceExhausted(Resource resource) {
        return new ResponseAcceptance(AcceptanceKind.RESOURCE_EXHAUSTED, Objects.requireNonNull(resource));
    }

    public AcceptanceKind kind() {
        return kind;
    }

    public Optional<Resource> resource() {
        return Optional.ofNullable(resource);
    }
}
