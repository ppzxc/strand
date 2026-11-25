package io.github.ppzxc.strand.netty.internal;

import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.core.internal.ErrorCode;

public final class WebSocketStatus {
    public static final int NONE = -1;

    private static final int NORMAL = 1000;
    private static final int PROTOCOL_ERROR = 1002;
    private static final int POLICY_VIOLATION = 1008;
    private static final int INTERNAL_ERROR = 1011;

    private WebSocketStatus() {}

    public static int of(CloseReason reason) {
        switch (reason.kind()) {
            case CLOSE:
                return NORMAL;
            case ERROR_CLOSE:
                return ofError(reason.errorCode());
            default:
                return NONE;
        }
    }

    private static int ofError(long errorCode) {
        if (errorCode == ErrorCode.EXCESSIVE_USE || errorCode == ErrorCode.UNAUTHENTICATED) {
            return POLICY_VIOLATION;
        }
        if (errorCode == ErrorCode.INTERNAL_ERROR || errorCode == ErrorCode.UNRESPONSIVE) {
            return INTERNAL_ERROR;
        }
        return PROTOCOL_ERROR;
    }
}
