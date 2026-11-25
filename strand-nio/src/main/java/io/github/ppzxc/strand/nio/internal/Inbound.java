package io.github.ppzxc.strand.nio.internal;

public final class Inbound {

    public enum Kind {
        NONE,

        BYTES,

        UNIT,

        UNSUPPORTED,

        END
    }

    private static final Inbound NONE = new Inbound(Kind.NONE, new byte[0]);
    private static final Inbound END = new Inbound(Kind.END, new byte[0]);
    private static final Inbound UNSUPPORTED = new Inbound(Kind.UNSUPPORTED, new byte[0]);

    private final Kind kind;
    private final byte[] data;

    private Inbound(Kind kind, byte[] data) {
        this.kind = kind;
        this.data = data;
    }

    public static Inbound none() {
        return NONE;
    }

    public static Inbound end() {
        return END;
    }

    public static Inbound bytes(byte[] bytes) {
        return new Inbound(Kind.BYTES, bytes);
    }

    public static Inbound unit(byte[] bytes) {
        return new Inbound(Kind.UNIT, bytes);
    }

    public static Inbound unsupported() {
        return UNSUPPORTED;
    }

    public Kind kind() {
        return kind;
    }

    public byte[] data() {
        return data;
    }
}
