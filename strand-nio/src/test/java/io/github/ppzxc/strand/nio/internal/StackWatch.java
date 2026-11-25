package io.github.ppzxc.strand.nio.internal;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

final class StackWatch {

    private static final long POLL_NANOS = TimeUnit.MICROSECONDS.toNanos(200);

    private StackWatch() {}

    static void awaitFrame(Thread thread, Predicate<StackTraceElement> frame) {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < limit) {
            for (StackTraceElement element : thread.getStackTrace()) {
                if (frame.test(element)) {
                    return;
                }
            }
            LockSupport.parkNanos(POLL_NANOS);
        }
        throw new AssertionError(thread.getName() + "의 스택에 기다리는 프레임이 5초 안에 나타나지 않았다");
    }

    static boolean inSelector(StackTraceElement element) {
        return element.getClassName().endsWith("SelectorImpl")
                && element.getMethodName().contains("elect");
    }

    static boolean inWriteWait(StackTraceElement element) {
        return element.getClassName().equals(TcpPassage.class.getName())
                && element.getMethodName().equals("await");
    }
}
