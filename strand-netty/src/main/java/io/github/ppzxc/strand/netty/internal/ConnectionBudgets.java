package io.github.ppzxc.strand.netty.internal;

import io.github.ppzxc.strand.api.ObservationBudget;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.api.ResourceUsage;
import java.util.EnumMap;
import java.util.Map;

/**
 * 연결 하나의 예약 한도. 예약은 연결 범위에서 먼저 하고 소유 객체 범위에서 이어 하며, 뒤쪽이 모자라면 앞쪽 몫을 돌려준다(부분 실패는 확보분을
 * 반환하고 기다리지 않는다).
 */
public final class ConnectionBudgets {
    /** 연결 범위와 소유 객체 범위가 함께 세는 종류. 관찰({@link Resource#OBSERVATIONS})은 {@link ObservationBudget}이 센다. */
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
