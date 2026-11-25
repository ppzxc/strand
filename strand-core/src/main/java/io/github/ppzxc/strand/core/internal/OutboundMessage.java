package io.github.ppzxc.strand.core.internal;

public final class OutboundMessage {

    private final byte[] bytes;

    private OutboundMessage(byte[] bytes) {
        this.bytes = bytes;
    }

    static OutboundMessage of(byte[] bytes) {
        return new OutboundMessage(bytes);
    }

    boolean isApplication() {
        return bytes.length >= Frames.HEADER_SIZE && Frames.typeOf(bytes, 0) >= FrameType.FIRST_APPLICATION;
    }

    long applicationStreamId() {
        return isApplication() ? Frames.streamIdOf(bytes, 0) : 0;
    }

    public byte[] bytes() {
        return bytes.clone();
    }

    public byte[] readOnlyBytes() {
        return bytes;
    }

    public int length() {
        return bytes.length;
    }
}
