package io.github.ppzxc.strand.api;

/** A request and its independently retained result. */
public interface RequestHandle {
    Result<RequestResult> result();

    void cancel();
}
