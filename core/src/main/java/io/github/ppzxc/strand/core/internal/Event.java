package io.github.ppzxc.strand.core.internal;

/** 엔드포인트가 애플리케이션에 전달하는 이벤트(적합성 벡터 형식 6.2절). */
public final class Event {

    /** 규격 2.5.1절의 {@code UNAVAILABLE}. */
    public static final int UNAVAILABLE = 14;

    /** 이벤트 종류. */
    public enum Kind {
        /** 자기가 보낸 요청의 결과가 애플리케이션에 전달됨. */
        RESULT,
        /** 연결이 닫혔음이 애플리케이션에 전달됨. */
        CLOSED
    }

    private final Kind kind;
    private final int streamId;
    private final int status;

    private Event(Kind kind, int streamId, int status) {
        this.kind = kind;
        this.streamId = streamId;
        this.status = status;
    }

    static Event closed() {
        return new Event(Kind.CLOSED, 0, 0);
    }

    static Event result(int streamId, int status) {
        return new Event(Kind.RESULT, streamId, status);
    }

    /**
     * 이벤트 종류.
     *
     * @return 종류
     */
    public Kind kind() {
        return kind;
    }

    /**
     * {@link Kind#RESULT}의 스트림 번호. 다른 종류에서는 0이다.
     *
     * @return 스트림 번호
     */
    public int streamId() {
        return streamId;
    }

    /**
     * {@link Kind#RESULT}의 status(규격 2.5.1절). 다른 종류에서는 0이다.
     *
     * @return status 값
     */
    public int status() {
        return status;
    }
}
