package io.github.ppzxc.strand.api;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.RejectedExecutionException;
import org.junit.jupiter.api.Test;

class ResultCompletionTest {
    @Test
    void cancelledSubmittedConversionRetainsSlotUntilExecutorRuns() {
        ObservationBudget connection = new ObservationBudget(1);
        ObservationBudget owner = new ObservationBudget(1);
        java.util.List<Runnable> queue = new java.util.ArrayList<>();
        ResultCompletion<String> result = new ResultCompletion<>(Runnable::run, connection, owner);
        java.util.concurrent.CompletableFuture<String> stage =
                result.toCompletionStage(queue::add).toCompletableFuture();
        result.complete("done");
        stage.cancel(false);
        assertTrue(result.observe(value -> {}).failureNow().isPresent());
        queue.remove(0).run();
        assertFalse(result.observe(value -> {}).failureNow().isPresent());
    }

    @Test
    void cancellingPendingConversionReleasesBothBudgets() {
        ObservationBudget connection = new ObservationBudget(1);
        ObservationBudget owner = new ObservationBudget(1);
        ResultCompletion<String> result = new ResultCompletion<>(Runnable::run, connection, owner);
        java.util.concurrent.CompletableFuture<String> stage =
                result.toCompletionStage(Runnable::run).toCompletableFuture();
        assertTrue(stage.cancel(false));
        Observation observation = result.observe(value -> {});
        assertFalse(observation.isDone());
        result.complete("done");
        assertTrue(observation.isDone());
    }

    @Test
    void boundsPendingObserversAndReleasesSlots() {
        ObservationBudget connection = new ObservationBudget(1);
        ObservationBudget owner = new ObservationBudget(1);
        ResultCompletion<String> result = new ResultCompletion<>(Runnable::run, connection, owner);
        Observation first = result.observe(value -> {});
        Observation rejected = result.observe(value -> fail("must not run"));
        assertTrue(rejected.failureNow().get() instanceof ResourceExhaustedException);
        assertFalse(first.isDone());
        assertEquals(1, connection.usage().current());
        assertEquals(1, connection.usage().highWater());
        assertEquals(1, connection.usage().rejections());
        result.complete("done");
        assertEquals(0, connection.usage().current());
        assertTrue(first.isDone());
        assertTrue(result.observe(value -> {}).isDone());
    }

    @Test
    void stageCancellationDoesNotCancelTheRetainedResult() {
        ResultCompletion<String> result =
                new ResultCompletion<>(Runnable::run, new ObservationBudget(10), new ObservationBudget(10));
        java.util.concurrent.CompletableFuture<String> cancelled =
                result.toCompletionStage(Runnable::run).toCompletableFuture();
        cancelled.cancel(false);
        result.complete("retained");
        assertEquals("retained", result.resultNow());
        assertEquals(
                "retained",
                result.toCompletionStage(Runnable::run).toCompletableFuture().join());
        assertTrue(result.toCompletionStage(command -> {
                    throw new RejectedExecutionException();
                })
                .toCompletableFuture()
                .isCompletedExceptionally());
    }

    @Test
    void observesAlreadyCompletedResult() {
        ResultCompletion<String> result =
                new ResultCompletion<>(Runnable::run, new ObservationBudget(10), new ObservationBudget(10));
        result.complete("done");
        Observation observation = result.observe(value -> assertEquals("done", value));
        assertTrue(observation.isDone());
        assertFalse(observation.failureNow().isPresent());
    }

    @Test
    void preservesFirstResultDespiteObserverFailures() {
        ResultCompletion<String> result =
                new ResultCompletion<>(Runnable::run, new ObservationBudget(10), new ObservationBudget(10));
        assertThrows(IllegalStateException.class, result::resultNow);
        Observation thrown = result.observe(value -> {
            throw new IllegalArgumentException("observer");
        });
        Observation rejected = result.observe(
                command -> {
                    throw new RejectedExecutionException();
                },
                value -> {});
        assertThrows(IllegalStateException.class, thrown::failureNow);
        assertTrue(result.complete("first"));
        assertFalse(result.complete("second"));
        assertEquals("first", result.resultNow());
        assertTrue(thrown.failureNow().get() instanceof IllegalArgumentException);
        assertTrue(rejected.failureNow().get() instanceof RejectedExecutionException);
        assertEquals(
                "first",
                result.toCompletionStage(Runnable::run).toCompletableFuture().join());
    }

    @Test
    void executorErrorDoesNotChangeResultNorSkipLaterObservers() {
        ObservationBudget connection = new ObservationBudget(2);
        ObservationBudget owner = new ObservationBudget(2);
        ResultCompletion<String> result = new ResultCompletion<>(Runnable::run, connection, owner);
        Observation first = result.observe(
                task -> {
                    throw new AssertionError();
                },
                value -> {});
        Observation second = result.observe(value -> {});
        assertTrue(result.complete("done"));
        assertEquals("done", result.resultNow());
        assertTrue(first.failureNow().get() instanceof Error);
        assertTrue(second.isDone());
        assertEquals(0, connection.usage().current());
    }

    @Test
    void listenRunsOnTheCompletingPathWithoutExecutorAndReleasesAfterRun() {
        ObservationBudget connection = new ObservationBudget(1);
        ObservationBudget owner = new ObservationBudget(1);
        ResultCompletion<String> result =
                new ResultCompletion<>(command -> fail("must not use the executor"), connection, owner);
        java.util.List<String> seen = new java.util.ArrayList<>();
        result.listen(seen::add);
        assertEquals(1, connection.usage().current());
        result.complete("done");
        assertEquals(java.util.Collections.singletonList("done"), seen);
        assertEquals(0, connection.usage().current());
        assertEquals(0, owner.usage().current());
    }

    @Test
    void listenOnCompletedResultRunsImmediately() {
        ResultCompletion<String> result =
                new ResultCompletion<>(Runnable::run, new ObservationBudget(1), new ObservationBudget(1));
        result.complete("done");
        java.util.List<String> seen = new java.util.ArrayList<>();
        result.listen(seen::add);
        assertEquals(java.util.Collections.singletonList("done"), seen);
    }

    @Test
    void listenFailsWhenSaturatedAndKeepsTheResult() {
        ObservationBudget connection = new ObservationBudget(1);
        ResultCompletion<String> result = new ResultCompletion<>(Runnable::run, connection, new ObservationBudget(1));
        result.listen(value -> {});
        ResourceExhaustedException error =
                assertThrows(ResourceExhaustedException.class, () -> result.listen(value -> fail("must not run")));
        assertEquals(Resource.OBSERVATIONS, error.resource());
        result.complete("kept");
        assertEquals("kept", result.resultNow());
    }

    @Test
    void cancellingAListeningReleasesTheReservationOnceAndLeavesTheResult() {
        ObservationBudget connection = new ObservationBudget(1);
        ObservationBudget owner = new ObservationBudget(1);
        ResultCompletion<String> result = new ResultCompletion<>(Runnable::run, connection, owner);
        ResultCompletion.Listening listening = result.listen(value -> fail("cancelled"));
        listening.cancel();
        listening.cancel();
        assertEquals(0, connection.usage().current());
        assertEquals(0, owner.usage().current());
        result.complete("done");
        assertEquals("done", result.resultNow());
    }

    @Test
    void aFailingListenerDoesNotChangeTheResultAndStillReleases() {
        ObservationBudget connection = new ObservationBudget(1);
        ResultCompletion<String> result = new ResultCompletion<>(Runnable::run, connection, new ObservationBudget(1));
        result.listen(value -> {
            throw new IllegalStateException("boom");
        });
        result.complete("done");
        assertEquals("done", result.resultNow());
        assertEquals(0, connection.usage().current());
    }
}
