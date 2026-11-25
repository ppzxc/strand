package io.github.ppzxc.strand.load;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Observation;
import io.github.ppzxc.strand.api.ObservationBudget;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.api.ResultCompletion;
import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.Status;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ObservationMonitorTest {
    @Test
    void slotExhaustionIsReportedOnceWithoutRunningTheResultCallback() throws Exception {
        ObservationBudget connectionBudget = new ObservationBudget(1);
        ObservationBudget ownerBudget = new ObservationBudget(1);
        ResultCompletion<RequestResult> result = new ResultCompletion<>(Runnable::run, connectionBudget, ownerBudget);
        result.observe(value -> {});
        result.observe(value -> {});
        Observation observation = result.observe(value -> {});
        assertThat(observation.failureNow()).isPresent();
        assertFailureReportedOnce(observation);
    }

    @Test
    void delayedCallbackSubmissionRejectionIsReportedOnce() throws Exception {
        ObservationBudget connectionBudget = new ObservationBudget(1);
        ObservationBudget ownerBudget = new ObservationBudget(1);
        ResultCompletion<RequestResult> result = new ResultCompletion<>(Runnable::run, connectionBudget, ownerBudget);
        java.util.concurrent.Executor rejected = command -> {
            throw new java.util.concurrent.RejectedExecutionException("executor stopped");
        };
        Observation observation = result.observe(rejected, value -> {});
        try (var scheduler = Executors.newSingleThreadScheduledExecutor()) {
            AtomicBoolean completed = new AtomicBoolean();
            AtomicInteger failures = new AtomicInteger();
            CountDownLatch failed = new CountDownLatch(1);
            ObservationMonitor.watch(observation, scheduler, completed, () -> {
                failures.incrementAndGet();
                failed.countDown();
            });

            result.complete(new RequestResult(Status.OK, Certainty.RESPONSE, new byte[0]));

            assertThat(failed.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(failures).hasValue(1);
            assertThat(completed).isTrue();
        }
    }

    private static void assertFailureReportedOnce(Observation observation) throws Exception {
        try (var scheduler = Executors.newSingleThreadScheduledExecutor()) {
            AtomicBoolean completed = new AtomicBoolean();
            AtomicInteger failures = new AtomicInteger();
            CountDownLatch failed = new CountDownLatch(1);
            ObservationMonitor.watch(observation, scheduler, completed, () -> {
                failures.incrementAndGet();
                failed.countDown();
            });

            assertThat(failed.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(failures).hasValue(1);
            assertThat(completed).isTrue();
        }
    }
}
