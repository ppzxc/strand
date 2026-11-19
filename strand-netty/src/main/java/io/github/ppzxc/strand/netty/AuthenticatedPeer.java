package io.github.ppzxc.strand.netty;

import java.net.SocketAddress;
import java.util.Objects;

/**
 * 연결에서 인증된 상대. 서버 연결에서는 {@link CredentialVerifier}가 받아들이며 돌려준 주체, 클라이언트 연결에서는 TLS가 확인한 서버 주체(TLS를 쓰지
 * 않으면 서버 주소)다.
 */
public final class AuthenticatedPeer {
    private final Object principal;
    private final SocketAddress remoteAddress;

    AuthenticatedPeer(Object principal, SocketAddress remoteAddress) {
        this.principal = Objects.requireNonNull(principal);
        this.remoteAddress = Objects.requireNonNull(remoteAddress);
    }

    /**
     * 인증된 주체.
     *
     * @return 주체
     */
    public Object principal() {
        return principal;
    }

    /**
     * 상대의 주소.
     *
     * @return 상대 주소
     */
    public SocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public String toString() {
        return "AuthenticatedPeer[remoteAddress=" + remoteAddress + "]";
    }
}
