package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.CloseNotice;

public final class ConnectionEndedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final transient CloseNotice notice;

    ConnectionEndedException(CloseNotice notice) {
        super("준비 중 연결이 끝났다: " + notice.kind());
        this.notice = notice;
    }

    public CloseNotice notice() {
        return notice;
    }
}
