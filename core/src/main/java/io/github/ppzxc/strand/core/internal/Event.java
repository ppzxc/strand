package io.github.ppzxc.strand.core.internal;

/** 엔드포인트가 애플리케이션에 전달하는 이벤트(적합성 벡터 형식 6.2절). */
public final class Event {

    /** 규격 2.5.1절의 {@code UNAVAILABLE}. */
    public static final int UNAVAILABLE = 14;

    /** 규격 2.5.1절의 {@code UNKNOWN}. 예약된 status는 이 값으로 취급한다. */
    private static final int UNKNOWN = 2;

    /** 규격 2.5.1절에 정의된 마지막 status({@code UNAUTHENTICATED}). */
    private static final int UNAUTHENTICATED = 16;

    /** 이벤트 종류. */
    public enum Kind {
        /** 자기가 보낸 요청의 결과가 애플리케이션에 전달됨. */
        RESULT,
        /** 연결이 닫혔음이 애플리케이션에 전달됨. */
        CLOSED,
        /** 상대가 연 스트림의 요청을 받음. */
        REQUEST_RECEIVED
    }

    private final Kind kind;
    private final int streamId;
    private final int status;
    private final int type;

    private Event(Kind kind, int streamId, int status, int type) {
        this.kind = kind;
        this.streamId = streamId;
        this.status = status;
        this.type = type;
    }

    static Event closed() {
        return new Event(Kind.CLOSED, 0, 0, 0);
    }

    static Event result(int streamId, int status) {
        return new Event(Kind.RESULT, streamId, status, 0);
    }

    /** 받은 status를 애플리케이션에 넘길 값으로 바꾼다. 예약된 값(17~255)은 {@code UNKNOWN}으로 취급한다(규격 2.5.1절). */
    static int normalizeStatus(int status) {
        return status > UNAUTHENTICATED ? UNKNOWN : status;
    }

    static Event requestReceived(int streamId, int type) {
        return new Event(Kind.REQUEST_RECEIVED, streamId, 0, type);
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
     * {@link Kind#RESULT}와 {@link Kind#REQUEST_RECEIVED}의 스트림 번호. 다른 종류에서는 0이다.
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

    /**
     * {@link Kind#REQUEST_RECEIVED}의 애플리케이션 {@code type}. 다른 종류에서는 0이다.
     *
     * @return type 값
     */
    public int type() {
        return type;
    }
}
