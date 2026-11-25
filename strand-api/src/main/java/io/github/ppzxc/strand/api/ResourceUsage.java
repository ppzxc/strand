package io.github.ppzxc.strand.api;

public final class ResourceUsage {
    private final long limit;
    private final long current;
    private final long highWater;
    private final long rejections;

    public ResourceUsage(long limit, long current, long highWater, long rejections) {
        this(1, limit, current, highWater, rejections);
    }

    private ResourceUsage(long minimumLimit, long limit, long current, long highWater, long rejections) {
        if (limit < minimumLimit || current < 0 || current > highWater || highWater > limit || rejections < 0) {
            throw new IllegalArgumentException("inconsistent resource usage");
        }
        this.limit = limit;
        this.current = current;
        this.highWater = highWater;
        this.rejections = rejections;
    }

    public static ResourceUsage of(Resource resource, long limit, long current, long highWater, long rejections) {
        return new ResourceUsage(resource.isTlsProgress() ? 0 : 1, limit, current, highWater, rejections);
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
