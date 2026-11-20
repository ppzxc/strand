package io.github.ppzxc.strand.api;

/** Local disposition of a response attempt. */
public enum AcceptanceKind {
    ACCEPTED,
    ALREADY_RESPONDED,
    REQUEST_ENDED,
    RESOURCE_EXHAUSTED
}
