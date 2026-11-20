package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.api.ObservationBudget;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.ResourceUsage;
import java.util.EnumMap;
import java.util.Map;

/**
 * 소유 객체(클라이언트·서버) 전체 범위의 예약 한도. 연결마다 {@link #newConnection}으로 연결 범위 한도를 더해 함께 예약한다. 관찰은 공통 API의
 * {@link ObservationBudget}이 예약하므로 여기서 따로 세지 않는다.
 */
public final class OwnerBudgets {
    /** 소유 객체 범위에서만 세는 종류. */
    private static final Resource[] OWNER_ONLY = {
        Resource.PREPARING_CONNECTIONS,
        Resource.TOTAL_CONNECTIONS,
        Resource.RESOLVE_TASKS,
        Resource.AUTHENTICATION_TASKS,
        Resource.FACTORY_TASKS
    };

    private final Map<Resource, Budget> budgets = new EnumMap<>(Resource.class);
    private final ObservationBudget observations;
    private volatile Runnable releaseObserver = () -> {};

    public OwnerBudgets(ResourceLimits limits) {
        for (Resource resource : ConnectionBudgets.SHARED) {
            budgets.put(resource, new Budget(resource, limits.perOwner().get(resource)));
        }
        for (Resource resource : OWNER_ONLY) {
            budgets.put(resource, new Budget(resource, limits.perOwner().get(resource)));
        }
        this.observations = new ObservationBudget(limits.perOwner().get(Resource.OBSERVATIONS));
    }

    public ConnectionBudgets newConnection(ResourceLimits limits) {
        return new ConnectionBudgets(this, limits);
    }

    public ObservationBudget observationBudget() {
        return observations;
    }

    public boolean tryReserve(Resource resource, long amount) {
        return budget(resource).tryAcquire(amount);
    }

    public void release(Resource resource, long amount) {
        budget(resource).release(amount);
        releaseObserver.run();
    }

    /**
     * 예약이 돌아올 때마다 호출할 관찰자를 정한다. 전체 종료가 잔존 작업이 풀리길 기다릴 때 쓴다. 관찰자는 가볍고 막히지 않아야 하며 어느 스레드에서든 불린다.
     *
     * @param observer 관찰자
     */
    public void onRelease(Runnable observer) {
        this.releaseObserver = java.util.Objects.requireNonNull(observer);
    }

    /**
     * 소유 객체 범위의 현재 예약량.
     *
     * @param resource 종류
     * @return 현재 예약량
     */
    public long current(Resource resource) {
        return usage(resource).current();
    }

    public ResourceUsage usage(Resource resource) {
        return resource == Resource.OBSERVATIONS
                ? observations.usage()
                : budget(resource).usage();
    }

    Budget budget(Resource resource) {
        Budget budget = budgets.get(resource);
        if (budget == null) {
            throw new IllegalArgumentException("소유 객체 범위에 없는 종류: " + resource);
        }
        return budget;
    }
}
