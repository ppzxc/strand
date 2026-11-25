package io.github.ppzxc.strand.netty;

import io.netty.util.concurrent.Future;

public final class NettyConnectAttempt {
    private final Future<NettyStrandConnection> result;
    private final Future<Void> released;
    private final ConnectAttemptState state;

    NettyConnectAttempt(Future<NettyStrandConnection> result, Future<Void> released, ConnectAttemptState state) {
        this.result = result;
        this.released = released;
        this.state = state;
    }

    public Future<NettyStrandConnection> result() {
        return result;
    }

    public boolean cancel() {
        return state.cancel();
    }

    public Future<Void> transportReleased() {
        return released;
    }
}
