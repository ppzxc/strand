package io.github.ppzxc.strand.netty;

import org.jspecify.annotations.Nullable;

final class ConnectAttemptState {
    private boolean cancelRequested;
    private boolean confirmed;
    private @Nullable ConnectionEngine engine;

    boolean cancel() {
        ConnectionEngine target;
        synchronized (this) {
            if (confirmed) {
                return false;
            }
            if (cancelRequested) {
                return true;
            }
            cancelRequested = true;
            target = engine;
        }
        if (target != null) {
            target.cancelAttempt();
        }
        return true;
    }

    void bind(ConnectionEngine value) {
        boolean pending;
        synchronized (this) {
            engine = value;
            pending = cancelRequested;
        }
        if (pending) {
            value.cancelAttempt();
        }
    }

    synchronized void confirm() {
        confirmed = true;
    }
}
