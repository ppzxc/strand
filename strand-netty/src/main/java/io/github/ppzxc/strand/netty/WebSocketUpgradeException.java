package io.github.ppzxc.strand.netty;

import java.io.IOException;

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

    public int httpStatus() {
        return httpStatus;
    }
}
