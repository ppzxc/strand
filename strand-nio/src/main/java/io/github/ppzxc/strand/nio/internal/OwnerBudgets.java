package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.api.ObservationBudget;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.ResourceUsage;
import java.util.EnumMap;
import java.util.Map;

public final class OwnerBudgets {

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

    public void onRelease(Runnable observer) {
        this.releaseObserver = java.util.Objects.requireNonNull(observer);
    }

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
