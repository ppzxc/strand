package io.github.ppzxc.strand.nio;

@FunctionalInterface
public interface RequestHandler {

    void handle(IncomingRequest request) throws Exception;
}
