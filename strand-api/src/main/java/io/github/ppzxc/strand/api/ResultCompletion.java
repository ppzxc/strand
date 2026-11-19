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
    private final @Nullable ObservationBudget dedicated;
    private final List<Consumer<T>> listeners = new ArrayList<>();
    private @Nullable T value;
    private boolean done;

    public ResultCompletion(
            Executor defaultExecutor, ObservationBudget connectionBudget, ObservationBudget ownerBudget) {
        this(defaultExecutor, connectionBudget, ownerBudget, null);
    }

    /**
     * 일반 관찰 한도와 별도로 자리 하나({@code dedicated})를 이 결과에 둔다. 관찰은 이 자리를 먼저 쓰므로 일반 한도가 모두 차 있어도 한 건은 등록된다.
     * 자리가 쓰이는 동안 오는 관찰은 일반 한도를 쓴다. 연결의 기본 종료 알림 자리가 이렇게 확보된다.
     */
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

    /** 관찰 자리를 예약한다. 예약했으면 돌려줄 때 부를 동작을, 모자라면 null을 돌려준다. */
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
