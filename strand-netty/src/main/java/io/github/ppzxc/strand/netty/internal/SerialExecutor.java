package io.github.ppzxc.strand.netty.internal;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

public final class SerialExecutor implements Executor {
    private final Executor backing;
    private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
    private boolean scheduled;

    public SerialExecutor(Executor backing) {
        this.backing = Objects.requireNonNull(backing);
    }

    @Override
    public void execute(Runnable task) {
        Objects.requireNonNull(task);
        synchronized (this) {
            queue.add(task);
            if (!scheduled) {
                scheduled = true;
                try {
                    backing.execute(this::drain);
                } catch (RuntimeException | Error e) {
                    scheduled = false;
                    queue.remove(task);
                    throw e;
                }
            }
        }
    }

    synchronized int queuedTasks() {
        return queue.size();
    }

    @SuppressWarnings("EmptyCatch")
    private void drain() {
        for (; ; ) {
            Runnable task;
            synchronized (this) {
                task = queue.poll();
                if (task == null) {
                    scheduled = false;
                    return;
                }
            }
            try {
                task.run();
            } catch (Throwable ignored) {
            }
        }
    }

    public static void requireNotDirect(Executor executor) {
        Thread caller = Thread.currentThread();
        AtomicReference<Thread> ranOn = new AtomicReference<>();
        try {
            executor.execute(() -> ranOn.compareAndSet(null, Thread.currentThread()));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("실행기가 확인 작업을 받지 않는다", e);
        }
        if (ranOn.get() == caller) {
            throw new IllegalArgumentException("호출 스레드에서 바로 실행하는 실행기는 지원하지 않는다");
        }
    }
}
