package io.github.ppzxc.strand.core.internal;

/** 전송 모듈이 쓰는 내부 출력의 식별자와 불변 내용. */
public final class EndpointOutput {
    private final long id;
    private final byte[] bytes;

    EndpointOutput(long id, byte[] bytes) {
        this.id = id;
        this.bytes = bytes;
    }

    public long id() {
        return id;
    }

    public byte[] bytes() {
        return bytes.clone();
    }
}
