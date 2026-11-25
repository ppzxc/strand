package io.github.ppzxc.strand.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;

public final class ResultCompletion<T> implements Result<T> {
    private final Executor defaultExecutor;
    private final ObservationBudget connectionBudget;
    private final ObservationBudget ownerBudget;
    private final @Nullable ObservationBudget dedicated;
    private final List<Consumer<T>> listeners = new ArrayList<>();
    private @Nullable T value;
    private boolean done;

    public ResultCompletion(
            Executor defaultExecutor, ObservationBudget connectionBudget, ObservationBudget ownerBudget) {
        this(defaultExecutor, connectionBudget, ownerBudget, null);
    }

    public ResultCompletion(
            Executor defaultExecutor,
            ObservationBudget connectionBudget,
            ObservationBudget ownerBudget,
            @Nullable ObservationBudget dedicated) {
        this.defaultExecutor = Objects.requireNonNull(defaultExecutor);
        this.connectionBudget = Objects.requireNonNull(connectionBudget);
        this.ownerBudget = Objects.requireNonNull(ownerBudget);
        this.dedicated = dedicated;
    }

    private @Nullable Runnable reserve() {
        ObservationBudget slot = dedicated;
        if (slot != null && slot.acquire()) {
            return slot::release;
        }
        if (!connectionBudget.acquire()) return null;
        if (ownerBudget.acquire()) {
            return () -> {
                ownerBudget.release();
                connectionBudget.release();
            };
        }
        connectionBudget.release();
        return null;
    }

    @SuppressWarnings("EmptyCatch")
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
        Runnable release = reserve();
        if (release == null) {
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
                        release.run();
                    }
                });
            } catch (Throwable error) {
                observation.finish(error);
                release.run();
            }
        });
        return observation;
    }

    public interface Listening {

        void cancel();
    }

    @SuppressWarnings("EmptyCatch")
    public Listening listen(Consumer<? super T> listener) {
        Objects.requireNonNull(listener);
        Runnable release = reserve();
        if (release == null) throw new ResourceExhaustedException(Resource.OBSERVATIONS);
        Consumer<T> registered = result -> {
            try {
                listener.accept(result);
            } catch (Throwable ignored) {

            } finally {
                release.run();
            }
        };
        register(registered);
        return () -> {
            boolean removed;
            synchronized (this) {
                removed = listeners.remove(registered);
            }
            if (removed) release.run();
        };
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public CompletionStage<T> toCompletionStage(Executor executor) {
        Objects.requireNonNull(executor);
        CompletableFuture<T> future = new CompletableFuture<>();
        Runnable release = reserve();
        if (release == null) {
            future.completeExceptionally(new ResourceExhaustedException(Resource.OBSERVATIONS));
            return future;
        }
        Consumer<T> listener = result -> {
            try {
                executor.execute(() -> {
                    try {
                        future.complete(result);
                    } finally {
                        release.run();
                    }
                });
            } catch (Throwable error) {
                future.completeExceptionally(error);
                release.run();
            }
        };
        register(listener);
        future.whenComplete((value, error) -> {
            if (future.isCancelled()) {
                boolean removed;
                synchronized (this) {
                    removed = listeners.remove(listener);
                }
                if (removed) release.run();
            }
        });
        return future;
    }
}
