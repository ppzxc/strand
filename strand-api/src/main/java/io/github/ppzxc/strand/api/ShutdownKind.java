package io.github.ppzxc.strand.api;

/** Outcome fixed at the shutdown deadline. */
public enum ShutdownKind {
    COMPLETED,
    DEADLINE_EXCEEDED
}
