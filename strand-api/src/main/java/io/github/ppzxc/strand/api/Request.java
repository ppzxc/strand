package io.github.ppzxc.strand.api;

import java.util.Objects;

/** An outgoing application request. */
public final class Request {
    private final int type;
    private final byte[] payload;
    private final Extensions extensions;

    private Request(int type, byte[] payload, Extensions extensions) {
        this.type = type;
        this.payload = payload;
        this.extensions = extensions;
    }

    public static Request of(int type, byte[] payload) {
        if (type < 0x100 || type > 0xffff) {
            throw new IllegalArgumentException("type must be an application frame type");
        }
        return new Request(
                type,
                Objects.requireNonNull(payload, "payload").clone(),
                Extensions.builder().build());
    }

    public Request extensions(Extensions value) {
        return new Request(type, payload, Objects.requireNonNull(value));
    }

    public Extensions extensions() {
        return extensions;
    }

    public int type() {
        return type;
    }

    /**
     * 본문의 길이(바이트). 본문을 복사하지 않으므로 전송 모듈이 용량을 예약하기 전에 크기만 알 때 쓴다.
     *
     * @return 본문 길이
     */
    public int payloadLength() {
        return payload.length;
    }

    public byte[] payload() {
        return payload.clone();
    }
}
