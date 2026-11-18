package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.ppzxc.strand.api.Resource;
import io.github.ppzxc.strand.api.ResourceUsage;
import org.junit.jupiter.api.Test;

class BudgetTest {

    @Test
    void acquiresUpToTheLimitAndCountsRejections() {
        Budget budget = new Budget(Resource.OUTGOING_BYTES, 10);

        assertThat(budget.tryAcquire(6)).isTrue();
        assertThat(budget.tryAcquire(4)).isTrue();
        assertThat(budget.tryAcquire(1)).isFalse();
        assertThat(budget.tryAcquire(1)).isFalse();

        ResourceUsage usage = budget.usage();
        assertThat(usage.limit()).isEqualTo(10);
        assertThat(usage.current()).isEqualTo(10);
        assertThat(usage.highWater()).isEqualTo(10);
        assertThat(usage.rejections()).isEqualTo(2);
    }

    @Test
    void releaseFreesSpaceAndKeepsHighWater() {
        Budget budget = new Budget(Resource.OUTGOING_REQUESTS, 3);
        budget.tryAcquire(3);

        budget.release(2);

        assertThat(budget.usage().current()).isEqualTo(1);
        assertThat(budget.usage().highWater()).isEqualTo(3);
        assertThat(budget.tryAcquire(2)).isTrue();
    }

    @Test
    void zeroAmountAlwaysFitsAndLeavesNoTrace() {
        Budget budget = new Budget(Resource.RESPONSE_BYTES, 1);
        budget.tryAcquire(1);

        assertThat(budget.tryAcquire(0)).isTrue();
        assertThat(budget.usage().rejections()).isZero();
    }

    @Test
    void amountLargerThanTheLimitIsRejectedWithoutOverflow() {
        Budget budget = new Budget(Resource.OUTGOING_BYTES, Long.MAX_VALUE);
        budget.tryAcquire(Long.MAX_VALUE - 1);

        assertThat(budget.tryAcquire(Long.MAX_VALUE)).isFalse();
        assertThat(budget.usage().current()).isEqualTo(Long.MAX_VALUE - 1);
    }

    @Test
    void releasingMoreThanHeldIsABug() {
        Budget budget = new Budget(Resource.HANDLER_TASKS, 2);
        budget.tryAcquire(1);

        assertThatThrownBy(() -> budget.release(2)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void negativeAmountsAreRejected() {
        Budget budget = new Budget(Resource.HANDLER_TASKS, 2);

        assertThatThrownBy(() -> budget.tryAcquire(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> budget.release(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nonPositiveLimitIsRejected() {
        assertThatThrownBy(() -> new Budget(Resource.HANDLER_TASKS, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
