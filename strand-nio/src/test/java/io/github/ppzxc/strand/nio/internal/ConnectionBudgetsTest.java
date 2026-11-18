package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.ConnectionLimits;
import io.github.ppzxc.strand.api.OwnerLimits;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceLimits;
import org.junit.jupiter.api.Test;

class ConnectionBudgetsTest {

    static ResourceLimits limits(long connectionCount, long ownerCount) {
        return ResourceLimits.builder()
                .perConnection(ConnectionLimits.builder()
                        .outgoingRequests(connectionCount, 100)
                        .incomingRequests(connectionCount, 100)
                        .responseBytes(100)
                        .handlerTasks(connectionCount)
                        .observations(connectionCount)
                        .preparationBytes(100)
                        .progressBytes(1000, 1000, 1000, 1000, 1000, 1000)
                        .build())
                .perOwner(OwnerLimits.builder()
                        .preparingConnections(ownerCount)
                        .totalConnections(ownerCount)
                        .preparationTasks(ownerCount, ownerCount, ownerCount)
                        .outgoingRequests(ownerCount, 1000)
                        .incomingRequests(ownerCount, 1000)
                        .responseBytes(1000)
                        .handlerTasks(ownerCount)
                        .observations(ownerCount)
                        .preparationBytes(1000)
                        .progressBytes(10_000, 10_000, 10_000, 10_000, 10_000, 10_000)
                        .build())
                .build();
    }

    @Test
    void reservesInBothScopesAndReleasesBoth() {
        OwnerBudgets owner = new OwnerBudgets(limits(2, 3));
        ConnectionBudgets connection = owner.newConnection(limits(2, 3));

        assertThat(connection.tryReserve(Resource.OUTGOING_REQUESTS, 1)).isTrue();

        assertThat(connection.usage(Resource.OUTGOING_REQUESTS).current()).isEqualTo(1);
        assertThat(owner.usage(Resource.OUTGOING_REQUESTS).current()).isEqualTo(1);
        connection.release(Resource.OUTGOING_REQUESTS, 1);
        assertThat(connection.usage(Resource.OUTGOING_REQUESTS).current()).isZero();
        assertThat(owner.usage(Resource.OUTGOING_REQUESTS).current()).isZero();
    }

    @Test
    void connectionScopeRejectionLeavesTheOwnerUntouched() {
        OwnerBudgets owner = new OwnerBudgets(limits(1, 3));
        ConnectionBudgets connection = owner.newConnection(limits(1, 3));
        connection.tryReserve(Resource.HANDLER_TASKS, 1);

        assertThat(connection.tryReserve(Resource.HANDLER_TASKS, 1)).isFalse();

        assertThat(owner.usage(Resource.HANDLER_TASKS).current()).isEqualTo(1);
        assertThat(connection.usage(Resource.HANDLER_TASKS).rejections()).isEqualTo(1);
    }

    @Test
    void ownerScopeRejectionReturnsTheConnectionShare() {
        OwnerBudgets owner = new OwnerBudgets(limits(2, 1));
        ConnectionBudgets first = owner.newConnection(limits(2, 1));
        ConnectionBudgets second = owner.newConnection(limits(2, 1));
        first.tryReserve(Resource.HANDLER_TASKS, 1);

        assertThat(second.tryReserve(Resource.HANDLER_TASKS, 1)).isFalse();

        assertThat(second.usage(Resource.HANDLER_TASKS).current()).isZero();
        assertThat(owner.usage(Resource.HANDLER_TASKS).rejections()).isEqualTo(1);
    }

    @Test
    void ownerOnlyResourcesAreReservedOnTheOwner() {
        OwnerBudgets owner = new OwnerBudgets(limits(1, 1));

        assertThat(owner.tryReserve(Resource.PREPARING_CONNECTIONS, 1)).isTrue();
        assertThat(owner.tryReserve(Resource.PREPARING_CONNECTIONS, 1)).isFalse();
        owner.release(Resource.PREPARING_CONNECTIONS, 1);
        assertThat(owner.tryReserve(Resource.PREPARING_CONNECTIONS, 1)).isTrue();
    }

    @Test
    void observationBudgetsAreSharedWithTheResultObservationApi() {
        OwnerBudgets owner = new OwnerBudgets(limits(2, 3));
        ConnectionBudgets connection = owner.newConnection(limits(2, 3));

        assertThat(connection.observationBudget().usage().limit()).isEqualTo(2);
        assertThat(owner.observationBudget().usage().limit()).isEqualTo(3);
        assertThat(connection.usage(Resource.OBSERVATIONS).limit()).isEqualTo(2);
        assertThat(owner.usage(Resource.OBSERVATIONS).limit()).isEqualTo(3);
    }

    @Test
    void resourcesOutsideTheScopeAreRejectedAsProgrammingErrors() {
        OwnerBudgets owner = new OwnerBudgets(limits(2, 3));
        ConnectionBudgets connection = owner.newConnection(limits(2, 3));

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> owner.tryReserve(Resource.PEER_FRAME_SIZE, 1))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> connection.usage(Resource.PREPARING_CONNECTIONS))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> connection.tryReserve(Resource.TOTAL_CONNECTIONS, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
