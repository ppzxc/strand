package io.github.ppzxc.strand.netty;

/** 서버의 {@link CredentialVerifier}가 자격 증명을 거부했다. 자격 증명 원문은 담지 않는다. */
public final class AuthenticationRejectedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    AuthenticationRejectedException() {
        super("자격 증명이 거부됐다");
    }
}
