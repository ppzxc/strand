package io.github.ppzxc.strand.netty;

@FunctionalInterface
public interface RequestHandler {

    void handle(IncomingRequest request) throws Exception;
}
