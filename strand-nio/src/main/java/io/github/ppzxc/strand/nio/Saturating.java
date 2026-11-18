package io.github.ppzxc.strand.nio;

/** 넘치지 않는 시각 산술. 기한은 {@code long} 나노초이고 넘치면 끝없는 기한({@link Long#MAX_VALUE})이다. */
final class Saturating {
    private Saturating() {}

    /** {@code a + b}. 넘치면 {@link Long#MAX_VALUE}({@code b}가 0 이상일 때) 또는 {@link Long#MIN_VALUE}에서 멈춘다. */
    static long add(long a, long b) {
        long sum = a + b;
        if (((a ^ sum) & (b ^ sum)) < 0) {
            return a < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        return sum;
    }
}
