package io.github.ppzxc.strand.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

/** Transport-side completion of a retained result. */
public final class ResultCompletion<T> implements Result<T> {
    private final Executor defaultExecutor;
    private final ObservationBudget connectionBudget;
    private final ObservationBudget ownerBudget;
    private final List<Consumer<T>> listeners = new ArrayList<>();
    private @Nullable T value;
    private boolean done;

    public ResultCompletion(
            Executor defaultExecutor, ObservationBudget connectionBudget, ObservationBudget ownerBudget) {
        this.defaultExecutor = Objects.requireNonNull(defaultExecutor);
        this.connectionBudget = Objects.requireNonNull(connectionBudget);
        this.ownerBudget = Objects.requireNonNull(ownerBudget);
    }

    private boolean reserve() {
        if (!connectionBudget.acquire()) return false;
        if (ownerBudget.acquire()) return true;
        connectionBudget.release();
        return false;
    }

    private void release() {
        ownerBudget.release();
        connectionBudget.release();
    }

    public boolean complete(T result) {
        Objects.requireNonNull(result);
        List<Consumer<T>> pending;
        synchronized (this) {
            if (done) return false;
            value = result;
            done = true;
            pending = new ArrayList<>(listeners);
            listeners.clear();
        }
        for (Consumer<T> listener : pending) {
            try {
                listener.accept(result);
            } catch (Throwable ignored) {
                // a failing listener must not change the retained result or skip later listeners
            }
        }
        return true;
    }

    @Override
    public synchronized boolean isDone() {
        return done;
    }

    @Override
    public synchronized T resultNow() {
        if (!done) throw new IllegalStateException("result is not complete");
        return Objects.requireNonNull(value);
    }

    private void register(Consumer<T> listener) {
        T current;
        synchronized (this) {
            if (!done) {
                listeners.add(listener);
                return;
            }
            current = Objects.requireNonNull(value);
        }
        listener.accept(current);
    }

    @Override
    public Observation observe(Consumer<? super T> observer) {
        return observe(defaultExecutor, observer);
    }

    @Override
    public Observation observe(Executor executor, Consumer<? super T> observer) {
        Objects.requireNonNull(executor);
        Objects.requireNonNull(observer);
        Observation observation = new Observation();
        if (!reserve()) {
            observation.finish(new ResourceExhaustedException(Resource.OBSERVATIONS));
            return observation;
        }
        register(result -> {
            try {
                executor.execute(() -> {
                    try {
                        observer.accept(result);
                        observation.finish(null);
                    } catch (Throwable error) {
                        observation.finish(error);
                    } finally {
                        release();
                    }
                });
            } catch (Throwable error) {
                observation.finish(error);
                release();
            }
        });
        return observation;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public CompletionStage<T> toCompletionStage(Executor executor) {
        Objects.requireNonNull(executor);
        CompletableFuture<T> future = new CompletableFuture<>();
        if (!reserve()) {
            future.completeExceptionally(new ResourceExhaustedException(Resource.OBSERVATIONS));
            return future;
        }
        Consumer<T> listener = result -> {
            try {
                executor.execute(() -> {
                    try {
                        future.complete(result);
                    } finally {
                        release();
                    }
                });
            } catch (Throwable error) {
                future.completeExceptionally(error);
                release();
            }
        };
        register(listener);
        future.whenComplete((value, error) -> {
            if (future.isCancelled()) {
                boolean removed;
                synchronized (this) {
                    removed = listeners.remove(listener);
                }
                if (removed) release();
            }
        });
        return future;
    }
}
