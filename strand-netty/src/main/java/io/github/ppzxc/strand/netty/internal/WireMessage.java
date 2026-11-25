package io.github.ppzxc.strand.netty.internal;

public final class WireMessage {
    public enum Kind {
        BINARY,
        UNSUPPORTED,
        PEER_CLOSED
    }

    private static final byte[] NO_BYTES = new byte[0];
    private static final WireMessage UNSUPPORTED = new WireMessage(Kind.UNSUPPORTED, NO_BYTES);
    private static final WireMessage PEER_CLOSED = new WireMessage(Kind.PEER_CLOSED, NO_BYTES);

    private final Kind kind;
    private final byte[] bytes;

    private WireMessage(Kind kind, byte[] bytes) {
        this.kind = kind;
        this.bytes = bytes;
    }

    public static WireMessage binary(byte[] bytes) {
        return new WireMessage(Kind.BINARY, bytes);
    }

    public static WireMessage unsupported() {
        return UNSUPPORTED;
    }

    public static WireMessage peerClosed() {
        return PEER_CLOSED;
    }

    public Kind kind() {
        return kind;
    }

    public byte[] bytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return kind + (kind == Kind.BINARY ? "(" + bytes.length + "B)" : "");
    }
}
