package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceUsage;
import java.util.Objects;

final class Budget {
    private final Resource resource;
    private final long limit;
    private long current;
    private long highWater;
    private long rejections;

    Budget(Resource resource, long limit) {

        boolean tls = resource == Resource.TLS_PLAINTEXT_BYTES
                || resource == Resource.TLS_CIPHERTEXT_BYTES
                || resource == Resource.TLS_CONTROL_BYTES;
        if (limit < 0 || (limit == 0 && !tls)) {
            throw new IllegalArgumentException(resource + " 한도는 양수여야 한다: " + limit);
        }
        this.resource = Objects.requireNonNull(resource);
        this.limit = limit;
    }

    Resource resource() {
        return resource;
    }

    synchronized boolean tryAcquire(long amount) {
        if (amount < 0) {
            throw new IllegalArgumentException("예약량은 음수일 수 없다: " + amount);
        }
        if (amount == 0) {
            return true;
        }

        if (amount > limit - current) {
            rejections++;
            return false;
        }
        current += amount;
        highWater = Math.max(highWater, current);
        return true;
    }

    synchronized void release(long amount) {
        if (amount < 0) {
            throw new IllegalArgumentException("반환량은 음수일 수 없다: " + amount);
        }
        if (amount > current) {
            throw new IllegalStateException(resource + " 예약보다 많이 반환했다: " + amount + " > " + current);
        }
        current -= amount;
    }

    synchronized ResourceUsage usage() {
        return ResourceUsage.of(resource, limit, current, highWater, rejections);
    }
}
