package io.github.ppzxc.strand.netty;

import java.net.InetAddress;
import java.net.UnknownHostException;

@FunctionalInterface
interface HostResolver {
    HostResolver SYSTEM = InetAddress::getAllByName;

    InetAddress[] resolve(String host) throws UnknownHostException;
}
