package io.github.ppzxc.strand.api;

/** Snapshot of a configured resource limit and its reservation statistics. */
public final class ResourceUsage {
    private final long limit;
    private final long current;
    private final long highWater;
    private final long rejections;

    public ResourceUsage(long limit, long current, long highWater, long rejections) {
        if (limit < 0 || current < 0 || current > highWater || highWater > limit || rejections < 0) {
            throw new IllegalArgumentException("inconsistent resource usage");
        }
        this.limit = limit;
        this.current = current;
        this.highWater = highWater;
        this.rejections = rejections;
    }

    public long limit() {
        return limit;
    }

    public long current() {
        return current;
    }

    public long highWater() {
        return highWater;
    }

    public long rejections() {
        return rejections;
    }
}
