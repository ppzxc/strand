package io.github.ppzxc.strand.api;

public final class ObservationBudget {
    private final long limit;
    private long used;
    private long highWater;
    private long rejections;

    public ObservationBudget(long limit) {
        if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
        this.limit = limit;
    }

    synchronized boolean acquire() {
        if (used == limit) {
            if (rejections < Long.MAX_VALUE) rejections++;
            return false;
        }
        used++;
        highWater = Math.max(highWater, used);
        return true;
    }

    public synchronized ResourceUsage usage() {
        return new ResourceUsage(limit, used, highWater, rejections);
    }

    synchronized void release() {
        used--;
    }
}
