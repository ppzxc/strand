package io.github.ppzxc.strand.nio.internal;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 앱이 준 실행기 위에서 연결 하나의 사용자 코드(인증·팩터리·핸들러·알림·기본 관찰)를 한 번에 하나씩, 낸 순서대로 돌린다. 비동기 업무가 끝나길 기다리지
 * 않고 호출이 돌아오면 다음 것을 돌린다. 전송 스레드에서 직접 실행하지 않는다.
 *
 * <p>대기 작업은 호출자가 먼저 예약한 한도(핸들러·관찰·준비 작업 슬롯)가 세므로 이 큐는 그 한도를 넘지 않는다. 뒤받침 실행기가 거절하면 그 작업은 큐에
 * 남기지 않고 예외를 호출자에게 그대로 던진다. 호출자는 그 작업의 예약을 돌려주고 실패를 관찰한다.
 */
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
        boolean schedule;
        synchronized (this) {
            queue.add(task);
            schedule = !scheduled;
            scheduled = true;
        }
        if (schedule) {
            try {
                backing.execute(this::drain);
            } catch (RuntimeException | Error e) {
                synchronized (this) {
                    // 방금 넣은 작업이 첫 작업이어야 한다. 큐에 남기지 않는다.
                    queue.clear();
                    scheduled = false;
                }
                throw e;
            }
        }
    }

    synchronized int queuedTasks() {
        return queue.size();
    }

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
                // 사용자 코드의 실패는 이 작업을 건 쪽이 관찰한다. 뒤의 작업은 계속 돌린다.
            }
        }
    }

    /**
     * 실행기가 호출 스레드에서 바로 작업을 실행하면 거절한다. 직접 실행 실행기는 전송 스레드에서 사용자 코드가 돌아 지원하지 않는다. 확인용 작업 하나를
     * 이 실행기에 낸다.
     *
     * @throws IllegalArgumentException 작업이 {@code execute} 안에서 호출 스레드에 실행됐거나 실행기가 확인 작업을 거절할 때
     */
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
