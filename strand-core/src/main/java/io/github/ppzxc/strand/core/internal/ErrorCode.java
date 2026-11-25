package io.github.ppzxc.strand.core.internal;

public final class ErrorCode {

    public static final int NO_ERROR = 0;
    public static final int PROTOCOL_ERROR = 1;
    public static final int INTERNAL_ERROR = 2;
    public static final int REFUSED = 3;
    public static final int CANCEL = 4;
    public static final int EXCESSIVE_USE = 5;
    public static final int UNRESPONSIVE = 6;
    public static final int UNAUTHENTICATED = 7;

    static final int RESERVED_FROM = 8;

    private ErrorCode() {}

    public static boolean isCloseCode(int errorCode) {
        switch (errorCode) {
            case NO_ERROR:
            case PROTOCOL_ERROR:
            case INTERNAL_ERROR:
            case EXCESSIVE_USE:
            case UNRESPONSIVE:
            case UNAUTHENTICATED:
                return true;
            default:
                return false;
        }
    }

    static long normalizeReceived(long errorCode) {
        return errorCode >= RESERVED_FROM ? INTERNAL_ERROR : errorCode;
    }
}
