package io.github.ppzxc.strand.core.internal;

/** 엔드포인트가 상대에게 보내는 메시지 하나(규격 1.2절). 바이너리 메시지 또는 WebSocket Close다. */
public final class OutboundMessage {

    /** 메시지 종류. */
    public enum Kind {
        /** 프레임 하나(연결 서문 포함)를 실은 바이너리 메시지. */
        BINARY,
        /** WebSocket 닫기({@code Close Frame 0x08}). WebSocket 전송에서만 나온다. */
        WEBSOCKET_CLOSE
    }

    private static final int NORMAL_CLOSURE = 1000;
    private static final byte[] NO_BYTES = new byte[0];

    private final Kind kind;
    private final byte[] bytes;
    private final int webSocketCloseCode;

    private OutboundMessage(Kind kind, byte[] bytes, int webSocketCloseCode) {
        this.kind = kind;
        this.bytes = bytes;
        this.webSocketCloseCode = webSocketCloseCode;
    }

    static OutboundMessage binary(byte[] bytes) {
        return new OutboundMessage(Kind.BINARY, bytes, 0);
    }

    /** {@code NO_ERROR} 종료의 WebSocket Close. 상태 코드는 {@code 1000 Normal Closure}다(규격 1.2절). */
    static OutboundMessage normalWebSocketClose() {
        return new OutboundMessage(Kind.WEBSOCKET_CLOSE, NO_BYTES, NORMAL_CLOSURE);
    }

    /**
     * 메시지 종류.
     *
     * @return 종류
     */
    public Kind kind() {
        return kind;
    }

    /**
     * {@link Kind#BINARY}의 내용. {@link Kind#WEBSOCKET_CLOSE}에서는 빈 배열이다.
     *
     * @return 메시지 내용
     */
    public byte[] bytes() {
        return bytes.clone();
    }

    /**
     * {@link Kind#WEBSOCKET_CLOSE}의 WebSocket 상태 코드. {@link Kind#BINARY}에서는 0이다.
     *
     * @return 상태 코드
     */
    public int webSocketCloseCode() {
        return webSocketCloseCode;
    }
}
