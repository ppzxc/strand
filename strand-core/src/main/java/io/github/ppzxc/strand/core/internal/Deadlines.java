package io.github.ppzxc.strand.core.internal;

/** 가상 시각 산술. 넘치면 {@link Long#MAX_VALUE}에서 멈춘다. */
final class Deadlines {

    static final long NANOS_PER_MILLI = 1_000_000L;

    private Deadlines() {}

    /** 밀리초를 나노초로 바꾼다. 넘치면 {@link Long#MAX_VALUE}에서 멈춘다. */
    static long millisToNanos(long ms) {
        return ms > Long.MAX_VALUE / NANOS_PER_MILLI ? Long.MAX_VALUE : ms * NANOS_PER_MILLI;
    }

    /** 기한이 지난 첫 밀리초(기한 정각은 지나지 않은 것이다). */
    static long after(long deadline) {
        return plus(deadline, 1);
    }

    static long plus(long at, long ms) {
        long sum = at + ms;
        return sum < at ? Long.MAX_VALUE : sum;
    }
}
