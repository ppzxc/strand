package io.github.ppzxc.strand.core.internal;

/** 응답 순서. JMH {@code @Param}이 쓰려면 공개 타입이어야 한다. */
public enum ResponseOrder {
    /** 요청이 도착한 순서(먼저 보낸 것부터). */
    OLDEST_FIRST,
    /** 도착의 반대 순서(나중 보낸 것부터). */
    NEWEST_FIRST
}
