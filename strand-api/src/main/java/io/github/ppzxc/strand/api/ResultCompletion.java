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

    /** 결과 확정 경로에서 직접 실행되는 등록의 취소 손잡이. */
    public interface Listening {
        /** 아직 실행되지 않았다면 등록을 제거하고 예약을 한 번 돌려준다. 이미 실행되었거나 반복해도 효과가 없다. */
        void cancel();
    }

    /**
     * 전송 모듈 내부용 등록이다. 앱 실행기를 거치지 않고 결과를 확정하는 스레드에서(이미 확정되었으면 호출 스레드에서) {@code listener}를 바로 실행한다. 관찰과 같은 연결·소유 객체
     * 한도를 등록 시 예약하고 {@code listener}가 끝나면 돌려준다. 예외는 결과를 바꾸지 않는다.
     *
     * @throws ResourceExhaustedException 관찰 한도가 차서 등록하지 못한 경우
     */
    public Listening listen(Consumer<? super T> listener) {
        Objects.requireNonNull(listener);
        if (!reserve()) throw new ResourceExhaustedException(Resource.OBSERVATIONS);
        Consumer<T> registered = result -> {
            try {
                listener.accept(result);
            } catch (Throwable ignored) {
                // a failing listener must not change the retained result
            } finally {
                release();
            }
        };
        register(registered);
        return () -> {
            boolean removed;
            synchronized (this) {
                removed = listeners.remove(registered);
            }
            if (removed) release();
        };
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
