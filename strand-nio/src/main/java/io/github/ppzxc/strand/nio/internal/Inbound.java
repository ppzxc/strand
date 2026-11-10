package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.WebSocketMessageKind;

/** {@link Passage#await}가 읽어 온 것. 바이트, 메시지, 끝남(EOF·상대가 닫음), 아무것도 없음 가운데 하나다. */
public final class Inbound {

    /** 읽어 온 것의 종류. */
    public enum Kind {
        /** 읽을 것이 없이 깨어남. */
        NONE,
        /** 바이트 스트림에서 읽은 바이트. 임의 위치에서 잘려 있을 수 있다. */
        BYTES,
        /** 메시지 하나. */
        MESSAGE,
        /** 상대가 끊었거나 닫음. */
        END
    }

    private static final Inbound NONE = new Inbound(Kind.NONE, new byte[0], WebSocketMessageKind.BINARY);
    private static final Inbound END = new Inbound(Kind.END, new byte[0], WebSocketMessageKind.BINARY);

    private final Kind kind;
    private final byte[] data;
    private final WebSocketMessageKind messageKind;

    private Inbound(Kind kind, byte[] data, WebSocketMessageKind messageKind) {
        this.kind = kind;
        this.data = data;
        this.messageKind = messageKind;
    }

    /**
     * 읽을 것이 없이 깨어남.
     *
     * @return 아무것도 없음
     */
    public static Inbound none() {
        return NONE;
    }

    /**
     * 상대가 끊었거나 닫음.
     *
     * @return 끝남
     */
    public static Inbound end() {
        return END;
    }

    /**
     * 바이트 스트림에서 읽은 바이트.
     *
     * @param bytes 읽은 바이트. 넘긴 뒤 통로가 다시 쓰지 않는 배열이어야 한다
     * @return 바이트
     */
    public static Inbound bytes(byte[] bytes) {
        return new Inbound(Kind.BYTES, bytes, WebSocketMessageKind.BINARY);
    }

    /**
     * 메시지 하나.
     *
     * @param kind 메시지 종류
     * @param message 메시지 내용. 넘긴 뒤 통로가 다시 쓰지 않는 배열이어야 한다
     * @return 메시지
     */
    public static Inbound message(WebSocketMessageKind kind, byte[] message) {
        return new Inbound(Kind.MESSAGE, message, kind);
    }

    /**
     * 읽어 온 것의 종류.
     *
     * @return 종류
     */
    public Kind kind() {
        return kind;
    }

    /**
     * {@link Kind#BYTES}의 바이트 또는 {@link Kind#MESSAGE}의 내용. 다른 종류에서는 빈 배열이다.
     *
     * @return 내용
     */
    public byte[] data() {
        return data;
    }

    /**
     * {@link Kind#MESSAGE}의 메시지 종류. 다른 종류에서는 의미가 없다.
     *
     * @return 메시지 종류
     */
    public WebSocketMessageKind messageKind() {
        return messageKind;
    }
}
