package io.github.ppzxc.strand.nio;

public final class AuthenticationRejectedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    AuthenticationRejectedException() {
        super("자격 증명이 거부됐다");
    }
}
