package io.github.ppzxc.strand.nio;

@FunctionalInterface
public interface ConnectionHandlerFactory {

    PreparedHandler create(NioStrandConnection connection) throws Exception;
}
