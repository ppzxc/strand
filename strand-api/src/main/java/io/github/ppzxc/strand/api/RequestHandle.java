package io.github.ppzxc.strand.api;

public interface RequestHandle {

    Result<RequestResult> result();

    void cancel();
}
