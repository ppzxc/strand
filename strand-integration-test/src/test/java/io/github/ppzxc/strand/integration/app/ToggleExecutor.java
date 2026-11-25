package io.github.ppzxc.strand.integration.app;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

final class ToggleExecutor implements Executor {
    private final Executor delegate;
    private final List<Runnable> stalledTasks = new ArrayList<>();
    private volatile boolean rejecting;
    private boolean stalled;

    ToggleExecutor(Executor delegate) {
        this.delegate = delegate;
    }

    void rejecting(boolean value) {
        this.rejecting = value;
    }

    synchronized void stalled(boolean value) {
        this.stalled = value;
    }

    void releaseStalled() {
        List<Runnable> tasks;
        synchronized (this) {
            stalled = false;
            tasks = new ArrayList<>(stalledTasks);
            stalledTasks.clear();
        }
        for (Runnable task : tasks) {
            delegate.execute(task);
        }
    }

    synchronized int stalledCount() {
        return stalledTasks.size();
    }

    @Override
    public void execute(Runnable task) {
        if (rejecting) {
            throw new RejectedExecutionException("테스트가 거절한다");
        }
        synchronized (this) {
            if (stalled) {
                stalledTasks.add(task);
                return;
            }
        }
        delegate.execute(task);
    }
}
