package io.github.ppzxc.strand.nio;

import org.jspecify.annotations.Nullable;

final class ConnectAttemptState {
    private boolean cancelRequested;
    private boolean confirmed;
    private @Nullable Runnable inProgress;
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

            if (inProgress != null) {
                inProgress.run();
            }
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

    synchronized boolean cancelRequested() {
        return cancelRequested;
    }

    synchronized void confirm() {
        confirmed = true;
    }

    void registerAbort(@Nullable Runnable abort) {
        synchronized (this) {
            inProgress = abort;
            if (abort != null && cancelRequested) {
                abort.run();
            }
        }
    }
}
