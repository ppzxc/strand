package io.github.ppzxc.strand.core.internal;

/** 엔드포인트가 애플리케이션에 전달하는 이벤트(적합성 벡터 형식 6.2절). */
public final class Event {

    /** 이벤트 종류. */
    public enum Kind {
        /** 연결이 닫혔음이 애플리케이션에 전달됨. */
        CLOSED
    }

    private final Kind kind;

    private Event(Kind kind) {
        this.kind = kind;
    }

    static Event closed() {
        return new Event(Kind.CLOSED);
    }

    /**
     * 이벤트 종류.
     *
     * @return 종류
     */
    public Kind kind() {
        return kind;
    }
}
