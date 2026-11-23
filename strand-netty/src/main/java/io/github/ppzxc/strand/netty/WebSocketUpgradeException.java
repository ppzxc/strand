package io.github.ppzxc.strand.netty;

import java.io.IOException;

/** WebSocket 업그레이드가 끝나지 못했다: 경로·하위 프로토콜 불일치, 상대의 거절, 형식이 어긋난 응답. {@link PreparationStage#UPGRADE}의 원인이다. */
public final class WebSocketUpgradeException extends IOException {
    private static final long serialVersionUID = 1L;

    private final int httpStatus;

    WebSocketUpgradeException(String message, int httpStatus, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
    }

    WebSocketUpgradeException(String message, int httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }

    /**
     * 이 쪽이 거절하며 보낸 HTTP 상태(서버), 또는 상대가 응답한 HTTP 상태(클라이언트).
     *
     * @return HTTP 상태. 알 수 없으면 0
     */
    public int httpStatus() {
        return httpStatus;
    }
}
