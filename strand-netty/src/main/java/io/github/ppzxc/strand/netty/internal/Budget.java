package io.github.ppzxc.strand.netty.internal;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceUsage;
import java.util.Objects;

/**
 * 한 종류·한 범위의 유한한 예약 한도. 현재량·최고량·거절 횟수만 유한하게 집계한다. 기다리지 않는다. 모자라면 곧바로 거절한다. 어느 스레드에서든
 * 부를 수 있다.
 */
final class Budget {
    private final Resource resource;
    private final long limit;
    private long current;
    private long highWater;
    private long rejections;

    Budget(Resource resource, long limit) {
        // 평문 연결은 TLS 공간이 필요 없어 그 한도가 0일 수 있다.
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

    /**
     * {@code amount}를 예약한다. 0은 늘 들어가며 흔적을 남기지 않는다.
     *
     * @return 예약했으면 true. 한도를 넘으면 false이고 아무것도 예약하지 않으며 거절 횟수만 는다
     */
    synchronized boolean tryAcquire(long amount) {
        if (amount < 0) {
            throw new IllegalArgumentException("예약량은 음수일 수 없다: " + amount);
        }
        if (amount == 0) {
            return true;
        }
        // 더하기가 넘치지 않게 남은 공간과 비교한다.
        if (amount > limit - current) {
            rejections++;
            return false;
        }
        current += amount;
        highWater = Math.max(highWater, current);
        return true;
    }

    /**
     * 예약한 양을 돌려준다.
     *
     * @throws IllegalStateException 예약한 것보다 많이 돌려줄 때(중복 반환)
     */
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
