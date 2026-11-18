package io.github.ppzxc.strand.core.internal;

/** 전송 모듈이 쓰는 내부 출력의 식별자와 불변 내용. */
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

    public byte[] bytes() {
        return message.bytes();
    }

    public int length() {
        return message.length();
    }
}
