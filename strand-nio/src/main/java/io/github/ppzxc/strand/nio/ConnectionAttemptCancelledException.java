package io.github.ppzxc.strand.nio;

/** 앱이 {@link NioConnectAttempt#cancel()}로 연결 시도를 취소했고 그 취소가 성공보다 먼저 처리됐다. */
public final class ConnectionAttemptCancelledException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    ConnectionAttemptCancelledException() {
        super("연결 시도가 취소됐다");
    }
}
