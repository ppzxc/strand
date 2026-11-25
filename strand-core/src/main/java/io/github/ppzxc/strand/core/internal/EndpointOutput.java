package io.github.ppzxc.strand.core.internal;

public final class EndpointOutput {
    private final long id;
    private final OutboundMessage message;

    EndpointOutput(long id, OutboundMessage message) {
        this.id = id;
        this.message = message;
    }

    public long id() {
        return id;
    }

    public OutboundMessage message() {
        return message;
    }

    public long applicationStreamId() {
        return message.applicationStreamId();
    }

    public byte[] bytes() {
        return message.bytes();
    }

    public byte[] readOnlyBytes() {
        return message.readOnlyBytes();
    }

    public int length() {
        return message.length();
    }
}
