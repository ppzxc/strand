package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;

final class PreparationBytes {
    private final ConnectionBudgets budgets;
    private long held;

    PreparationBytes(ConnectionBudgets budgets) {
        this.budgets = budgets;
    }

    synchronized boolean tryReserve(long bytes) {
        if (!budgets.tryReserve(Resource.PREPARATION_BYTES, bytes)) {
            return false;
        }
        held += bytes;
        return true;
    }

    synchronized void release(long bytes) {
        if (bytes > 0) {
            budgets.release(Resource.PREPARATION_BYTES, bytes);
            held -= bytes;
        }
    }

    synchronized void releaseAll() {
        if (held > 0) {
            budgets.release(Resource.PREPARATION_BYTES, held);
            held = 0;
        }
    }
}
