package io.github.ppzxc.strand.api;

import java.util.Objects;
import java.util.Optional;
import org.jspecify.annotations.Nullable;

/** Retained reason for a connection ending, including locally observed failure. */
public final class CloseNotice {
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
    private final @Nullable Throwable cause;

    private CloseNotice(Kind kind, long errorCode, @Nullable Sender sender, @Nullable Throwable cause) {
        this.kind = kind;
        this.errorCode = errorCode;
        this.sender = sender;
        this.cause = cause;
    }

    public static CloseNotice close(Sender sender) {
        return new CloseNotice(Kind.CLOSE, 0, Objects.requireNonNull(sender), null);
    }

    public static CloseNotice errorClose(long errorCode, Sender sender, @Nullable Throwable cause) {
        if (errorCode <= 0 || errorCode > 0xffffffffL) throw new IllegalArgumentException("invalid error code");
        return new CloseNotice(Kind.ERROR_CLOSE, errorCode, Objects.requireNonNull(sender), cause);
    }

    public static CloseNotice disconnected(@Nullable Throwable cause) {
        return new CloseNotice(Kind.DISCONNECTED, 0, null, cause);
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

    public Optional<Throwable> cause() {
        return Optional.ofNullable(cause);
    }
}
