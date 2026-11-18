package io.github.ppzxc.strand.nio;

import io.github.ppzxc.strand.api.CloseNotice;

/** 연결 준비가 끝나기 전에 연결이 먼저 끝났음을 준비 실패의 원인으로 알린다. */
public final class ConnectionEndedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final transient CloseNotice notice;

    ConnectionEndedException(CloseNotice notice) {
        super("준비 중 연결이 끝났다: " + notice.kind());
        this.notice = notice;
    }

    /**
     * 연결이 끝난 원인.
     *
     * @return 종료 알림
     */
    public CloseNotice notice() {
        return notice;
    }
}
