package io.github.ppzxc.strand.nio;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.ObservationBudget;
import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResultCompletion;
import io.github.ppzxc.strand.nio.internal.ConnectionBudgets;
import io.github.ppzxc.strand.nio.internal.OwnerBudgets;
import org.junit.jupiter.api.Test;

class PreparationBytesAndIncomingStateTest {

    @Test
    void releasingEverythingTwiceReturnsTheReservationOnlyOnce() {
        AppFixture fixture = AppFixture.plain();
        try {
            OwnerBudgets owner = new OwnerBudgets(fixture.limits);
            ConnectionBudgets budgets = owner.newConnection(fixture.limits);
            PreparationBytes bytes = new PreparationBytes(budgets);
            assertThat(bytes.tryReserve(10)).isTrue();

            bytes.releaseAll();
            bytes.releaseAll();

            assertThat(owner.current(Resource.PREPARATION_BYTES)).isZero();
        } finally {
            fixture.close();
        }
    }

    @Test
    void theCountReturnIsClaimedOnlyOnce() {
        AppFixture fixture = AppFixture.plain();
        try {
            IncomingState state = new IncomingState(
                    1,
                    10,
                    new ResultCompletion<>(fixture.executor, new ObservationBudget(1), new ObservationBudget(1)));

            assertThat(state.claimCountReturn()).isTrue();
            assertThat(state.claimCountReturn()).isFalse();
        } finally {
            fixture.close();
        }
    }

    @Test
    void theBytesReturnWaitsForBothTheRequestEndAndTheHandlerAndIsClaimedOnlyOnce() {
        AppFixture fixture = AppFixture.plain();
        try {
            IncomingState state = new IncomingState(
                    1,
                    10,
                    new ResultCompletion<>(fixture.executor, new ObservationBudget(1), new ObservationBudget(1)));

            state.handlerSettled();
            assertThat(state.claimBytesReturn()).isFalse();

            assertThat(state.claimCountReturn()).isTrue();
            assertThat(state.claimBytesReturn()).isTrue();
            assertThat(state.claimBytesReturn()).isFalse();
        } finally {
            fixture.close();
        }
    }

    @Test
    void theBytesReturnWaitsForTheHandlerEvenAfterTheRequestEnded() {
        AppFixture fixture = AppFixture.plain();
        try {
            IncomingState state = new IncomingState(
                    1,
                    10,
                    new ResultCompletion<>(fixture.executor, new ObservationBudget(1), new ObservationBudget(1)));
            assertThat(state.claimCountReturn()).isTrue();

            assertThat(state.claimBytesReturn()).isFalse();
        } finally {
            fixture.close();
        }
    }
}
