package io.github.ppzxc.strand.nio;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class NioConnectAttempt {
    private final CompletionStage<NioStrandConnection> result;
    private final CompletableFuture<Void> released;
    private final ConnectAttemptState state;

    NioConnectAttempt(
            CompletionStage<NioStrandConnection> result, CompletableFuture<Void> released, ConnectAttemptState state) {
        this.result = result;
        this.released = released;
        this.state = state;
    }

    public CompletionStage<NioStrandConnection> result() {
        return result;
    }

    public boolean cancel() {
        return state.cancel();
    }

    public CompletionStage<Void> transportReleased() {
        return released;
    }
}
