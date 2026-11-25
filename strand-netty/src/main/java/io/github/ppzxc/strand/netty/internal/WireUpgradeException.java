package io.github.ppzxc.strand.netty.internal;

import java.io.IOException;

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

    public int httpStatus() {
        return httpStatus;
    }
}
