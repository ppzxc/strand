package io.github.ppzxc.strand.netty;

import java.net.SocketAddress;
import java.util.Objects;

public final class AuthenticatedPeer {
    private final Object principal;
    private final SocketAddress remoteAddress;

    AuthenticatedPeer(Object principal, SocketAddress remoteAddress) {
        this.principal = Objects.requireNonNull(principal);
        this.remoteAddress = Objects.requireNonNull(remoteAddress);
    }

    public Object principal() {
        return principal;
    }

    public SocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public String toString() {
        return "AuthenticatedPeer[remoteAddress=" + remoteAddress + "]";
    }
}
