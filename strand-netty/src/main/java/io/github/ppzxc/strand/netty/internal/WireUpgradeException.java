package io.github.ppzxc.strand.netty.internal;

import java.io.IOException;

/** 메시지 통로의 업그레이드(WebSocket 핸드셰이크)가 실패했다. 서버가 거절했다면 상대에게 보낸 HTTP 상태를 담는다(그 밖에는 0). */
public final class WireUpgradeException extends IOException {
    private static final long serialVersionUID = 1L;

    private final int httpStatus;

    WireUpgradeException(String message, int httpStatus, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
    }

    WireUpgradeException(String message, int httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }

    /** 상대에게 보냈거나 상대가 보낸 HTTP 상태. 알 수 없으면 0. */
    public int httpStatus() {
        return httpStatus;
    }
}
