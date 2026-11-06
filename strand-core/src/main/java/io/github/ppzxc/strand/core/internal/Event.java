package io.github.ppzxc.strand.core.internal;

/** 엔드포인트가 애플리케이션에 전달하는 이벤트(적합성 벡터 형식 6.2절). */
public final class Event {

    /** 응답 {@code type}이 요청과 다를 때 애플리케이션에 넘기는 로컬 status(규격 4.1절). 규격 표의 이름이 아니라 {@code INTERNAL}의 별칭이다. */
    static final int RESPONSE_TYPE_MISMATCH = Status.INTERNAL;

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

    /** 스트림을 열기 전에 로컬에서 실패한 요청의 결과. 스트림 번호가 없다(규격 2.2.5절, 3.2.1절, 3.5절). */
    static Event localUnavailable() {
        return new Event(Kind.RESULT, 0, Status.UNAVAILABLE, 0);
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
     * {@link Kind#RESULT}와 {@link Kind#REQUEST_RECEIVED}의 스트림 번호. 다른 종류에서는 0이다. 스트림을 열기 전에 로컬에서 실패한
     * 요청의 {@link Kind#RESULT}도 0이다. 실제 스트림 번호는 0이 될 수 없다.
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
