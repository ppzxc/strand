package io.github.ppzxc.strand.netty;

public final class ConnectionAttemptCancelledException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    ConnectionAttemptCancelledException() {
        super("연결 시도가 취소됐다");
    }
}
