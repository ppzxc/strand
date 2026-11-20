package io.github.ppzxc.strand.api;

import java.util.OptionalLong;

/** Why a received request ended. */
public final class RequestEnd {
    private enum Kind {
        RESPONSE_ACCEPTED,
        PEER_RESET,
        CONNECTION_END
    }

    private final Kind kind;
    private final long errorCode;

    private RequestEnd(Kind kind, long errorCode) {
        this.kind = kind;
        this.errorCode = errorCode;
    }

    public static RequestEnd responseAccepted() {
        return new RequestEnd(Kind.RESPONSE_ACCEPTED, 0);
    }

    public static RequestEnd connectionEnd() {
        return new RequestEnd(Kind.CONNECTION_END, 0);
    }

    public static RequestEnd peerReset(long errorCode) {
        if (errorCode < 0 || errorCode > 0xffffffffL) throw new IllegalArgumentException("invalid error code");
        return new RequestEnd(Kind.PEER_RESET, errorCode);
    }

    public boolean isResponseAccepted() {
        return kind == Kind.RESPONSE_ACCEPTED;
    }

    public boolean isPeerReset() {
        return kind == Kind.PEER_RESET;
    }

    public boolean isConnectionEnd() {
        return kind == Kind.CONNECTION_END;
    }

    public OptionalLong errorCode() {
        return isPeerReset() ? OptionalLong.of(errorCode) : OptionalLong.empty();
    }
}
