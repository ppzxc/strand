package io.github.ppzxc.strand.api;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public interface Result<T> {
    boolean isDone();

    T resultNow();

    Observation observe(Consumer<? super T> observer);

    Observation observe(Executor executor, Consumer<? super T> observer);

    CompletionStage<T> toCompletionStage(Executor executor);
}
