package io.github.ppzxc.strand.nio.internal;

/** {@link Passage#await}가 읽어 온 것. 바이트 스트림의 바이트, 한 단위, 바이트로 넘길 수 없는 입력, 끝남(EOF·상대가 닫음), 아무것도 없음 가운데 하나다. */
public final class Inbound {

    /** 읽어 온 것의 종류. */
    public enum Kind {
        /** 읽을 것이 없이 깨어남. */
        NONE,
        /** 바이트 스트림에서 읽은 바이트. 임의 위치에서 잘려 있을 수 있다. */
        BYTES,
        /** 정확히 한 단위(연결 서문 또는 프레임 하나)라고 주장하는 바이트. 메시지 통로가 메시지 하나를 이렇게 넘긴다. */
        UNIT,
        /** 바이트로 넘길 수 없는 입력(예: 텍스트 메시지). */
        UNSUPPORTED,
        /** 상대가 끊었거나 닫음. */
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
        return new Inbound(Kind.BYTES, bytes);
    }

    /**
     * 정확히 한 단위(연결 서문 또는 프레임 하나)라고 주장하는 바이트.
     *
     * @param bytes 한 단위의 바이트. 넘긴 뒤 통로가 다시 쓰지 않는 배열이어야 한다
     * @return 한 단위
     */
    public static Inbound unit(byte[] bytes) {
        return new Inbound(Kind.UNIT, bytes);
    }

    /**
     * 바이트로 넘길 수 없는 입력(예: 텍스트 메시지).
     *
     * @return 잘못된 입력
     */
    public static Inbound unsupported() {
        return UNSUPPORTED;
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
     * {@link Kind#BYTES}의 바이트 또는 {@link Kind#UNIT}의 한 단위. 다른 종류에서는 빈 배열이다.
     *
     * @return 내용
     */
    public byte[] data() {
        return data;
    }
}
