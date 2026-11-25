package io.github.ppzxc.strand.core.internal;

import java.util.Optional;
import org.jspecify.annotations.Nullable;

public final class CloseReason {

    public enum Kind {
        CLOSE,

        ERROR_CLOSE,

        DISCONNECTED
    }

    public enum Sender {
        LOCAL,

        PEER
    }

    private final Kind kind;
    private final long errorCode;
    private final @Nullable Sender sender;

    private CloseReason(Kind kind, long errorCode, @Nullable Sender sender) {
        this.kind = kind;
        this.errorCode = errorCode;
        this.sender = sender;
    }

    static CloseReason close(Sender sender) {
        return new CloseReason(Kind.CLOSE, ErrorCode.NO_ERROR, sender);
    }

    static CloseReason errorClose(long errorCode, Sender sender) {
        return new CloseReason(Kind.ERROR_CLOSE, errorCode, sender);
    }

    static CloseReason disconnected() {
        return new CloseReason(Kind.DISCONNECTED, ErrorCode.NO_ERROR, null);
    }

    public Kind kind() {
        return kind;
    }

    public long errorCode() {
        return errorCode;
    }

    public Optional<Sender> sender() {
        return Optional.ofNullable(sender);
    }
}
