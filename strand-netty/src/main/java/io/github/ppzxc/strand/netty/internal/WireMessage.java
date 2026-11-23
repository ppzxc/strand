package io.github.ppzxc.strand.netty.internal;

/** 메시지 통로가 연결 엔진에 넘기는 입력 하나. */
public final class WireMessage {
    /** 입력의 종류. */
    public enum Kind {
        /** 바이너리 메시지 하나(조각을 모은 결과). core의 단위 입력이다. */
        BINARY,
        /** 바이트로 넘길 수 없는 입력(텍스트 메시지, 한도를 넘은 메시지, 형식을 어긴 프레임). core에는 "받았다" 신호로 알린다. */
        UNSUPPORTED,
        /** 상대가 통로를 닫았다(Close 프레임). 바이트 스트림의 끊김과 같다. */
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

    /** 바이너리 메시지. */
    public static WireMessage binary(byte[] bytes) {
        return new WireMessage(Kind.BINARY, bytes);
    }

    /** 바이트로 넘길 수 없는 입력. */
    public static WireMessage unsupported() {
        return UNSUPPORTED;
    }

    /** 상대가 통로를 닫았다. */
    public static WireMessage peerClosed() {
        return PEER_CLOSED;
    }

    public Kind kind() {
        return kind;
    }

    /** 바이너리 메시지의 내용. 다른 종류는 빈 배열이다. */
    public byte[] bytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return kind + (kind == Kind.BINARY ? "(" + bytes.length + "B)" : "");
    }
}
