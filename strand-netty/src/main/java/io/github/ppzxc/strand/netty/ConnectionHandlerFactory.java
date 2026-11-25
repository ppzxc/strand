package io.github.ppzxc.strand.netty;

@FunctionalInterface
public interface ConnectionHandlerFactory {

    PreparedHandler create(NettyStrandConnection connection) throws Exception;
}
