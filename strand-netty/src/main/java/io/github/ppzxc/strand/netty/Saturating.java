package io.github.ppzxc.strand.netty;

final class Saturating {
    private Saturating() {}

    static long add(long a, long b) {
        long sum = a + b;
        if (((a ^ sum) & (b ^ sum)) < 0) {
            return a < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        return sum;
    }
}
