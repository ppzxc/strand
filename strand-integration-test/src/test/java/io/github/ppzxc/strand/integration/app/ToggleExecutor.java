package io.github.ppzxc.strand.integration.app;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/** 앱 실행기. 평소에는 위임하고, 시험이 거절하게 하거나(계속 거절) 받기만 하고 실행하지 않게(정체) 바꿀 수 있다. */
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

    /** 켜면 이후 작업을 받기만 하고 {@link #releaseStalled()} 전까지 실행하지 않는다. */
    synchronized void stalled(boolean value) {
        this.stalled = value;
    }

    /** 정체 중 받은 작업을 모두 실행기에 넘긴다. 정체는 끈다. */
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
