package io.github.ppzxc.strand.core.internal;

final class Deadlines {

    static final long NANOS_PER_MILLI = 1_000_000L;

    private Deadlines() {}

    static long millisToNanos(long ms) {
        return ms > Long.MAX_VALUE / NANOS_PER_MILLI ? Long.MAX_VALUE : ms * NANOS_PER_MILLI;
    }

    static long after(long deadline) {
        return plus(deadline, 1);
    }

    static long plus(long at, long ms) {
        long sum = at + ms;
        return sum < at ? Long.MAX_VALUE : sum;
    }
}
