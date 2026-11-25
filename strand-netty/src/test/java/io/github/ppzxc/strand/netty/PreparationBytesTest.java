package io.github.ppzxc.strand.netty;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.netty.internal.ConnectionBudgets;
import io.github.ppzxc.strand.netty.internal.OwnerBudgets;
import org.junit.jupiter.api.Test;

class PreparationBytesTest {

    private static ResourceLimits limits() {
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(2, 100)
                        .incomingRequests(2, 100)
                        .responseBytes(100)
                        .handlerTasks(2)
                        .observations(2)
                        .preparationBytes(10)
                        .progressBytes(1000, 1000, 1000, 1000, 1000, 1000)
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(2)
                        .totalConnections(2)
                        .preparationTasks(2, 2, 2)
                        .outgoingRequests(2, 1000)
                        .incomingRequests(2, 1000)
                        .responseBytes(1000)
                        .handlerTasks(2)
                        .observations(2)
                        .preparationBytes(1000)
                        .progressBytes(10_000, 10_000, 10_000, 10_000, 10_000, 10_000)
                        .build())
                .build();
    }

    @Test
    void reservesOnlyWhatFitsAndReturnsEverythingExactlyOnce() {
        OwnerBudgets owner = new OwnerBudgets(limits());
        ConnectionBudgets budgets = owner.newConnection(limits());
        PreparationBytes bytes = new PreparationBytes(budgets);

        assertThat(bytes.tryReserve(8)).isTrue();
        assertThat(bytes.tryReserve(8)).isFalse();
        bytes.release(0);
        assertThat(owner.usage(Resource.PREPARATION_BYTES).current()).isEqualTo(8);
        bytes.releaseAll();
        bytes.releaseAll();

        assertThat(owner.usage(Resource.PREPARATION_BYTES).current()).isZero();
    }
}
