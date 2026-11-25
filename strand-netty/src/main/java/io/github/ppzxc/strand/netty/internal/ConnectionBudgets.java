package io.github.ppzxc.strand.netty.internal;

import io.github.ppzxc.strand.api.ObservationBudget;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.ResourceUsage;
import java.util.EnumMap;
import java.util.Map;

public final class ConnectionBudgets {
    static final Resource[] SHARED = {
        Resource.OUTGOING_REQUESTS,
        Resource.OUTGOING_BYTES,
        Resource.INCOMING_REQUESTS,
        Resource.INCOMING_BYTES,
        Resource.RESPONSE_BYTES,
        Resource.HANDLER_TASKS,
        Resource.PREPARATION_BYTES,
        Resource.FRAME_ASSEMBLY_BYTES,
        Resource.CONTROL_OUTPUT_BYTES,
        Resource.RESULT_EVENT_BYTES,
        Resource.TLS_PLAINTEXT_BYTES,
        Resource.TLS_CIPHERTEXT_BYTES,
        Resource.TLS_CONTROL_BYTES
    };

    private final OwnerBudgets owner;
    private final Map<Resource, Budget> budgets = new EnumMap<>(Resource.class);
    private final ObservationBudget observations;

    ConnectionBudgets(OwnerBudgets owner, ResourceLimits limits) {
        this.owner = owner;
        for (Resource resource : SHARED) {
            budgets.put(resource, new Budget(resource, limits.perConnection().get(resource)));
        }
        this.observations = new ObservationBudget(limits.perConnection().get(Resource.OBSERVATIONS));
    }

    public boolean tryReserve(Resource resource, long amount) {
        if (!budget(resource).tryAcquire(amount)) {
            return false;
        }
        if (owner.tryReserve(resource, amount)) {
            return true;
        }
        budget(resource).release(amount);
        return false;
    }

    public void release(Resource resource, long amount) {
        owner.release(resource, amount);
        budget(resource).release(amount);
    }

    public ResourceUsage usage(Resource resource) {
        return resource == Resource.OBSERVATIONS
                ? observations.usage()
                : budget(resource).usage();
    }

    private Budget budget(Resource resource) {
        Budget budget = budgets.get(resource);
        if (budget == null) {
            throw new IllegalArgumentException("연결 범위에 없는 종류: " + resource);
        }
        return budget;
    }

    public ObservationBudget observationBudget() {
        return observations;
    }

    public OwnerBudgets owner() {
        return owner;
    }
}
