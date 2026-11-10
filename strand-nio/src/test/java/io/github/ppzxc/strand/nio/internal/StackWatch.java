package io.github.ppzxc.strand.nio.internal;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

/**
 * 스레드가 어떤 호출 안에서 기다리고 있는지 스택으로 확인한다. 테스트가 "기다리는 중에 닫는다"를 시간 추측 없이 만들려는 용도다. 기다림 자체를 감싸 알릴
 * 수 없는 JDK 호출(선택기의 {@code select})에 쓰고, 상한 시간(5초) 안에서만 짧게 쉬며 되풀이해 본다.
 */
final class StackWatch {

    private static final long POLL_NANOS = TimeUnit.MICROSECONDS.toNanos(200);

    private StackWatch() {}

    /** 주어진 스레드의 스택에 조건에 맞는 프레임이 나타날 때까지 기다린다. 이르지 못하면 실패한다. */
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

    /**
     * 선택기에서 기다리는 프레임인가(JDK 구현의 {@code SelectorImpl.select} 계열). JDK 내부 이름에 기대는 약점이 있다. 대안인 "선택 직전에 알리는 훅"은
     * 알린 뒤 {@code select}에 들어가기 전에 닫히는 틈이 남아, 이 시험이 증명하려는 "select 중에 닫으면 깨어난다"를 흐린다. 그래서 스택 확인을 유지하고,
     * JDK 구현 이름이 바뀌어 프레임을 못 찾으면 5초 뒤 명확히 실패한다(조용히 통과하지 않는다).
     */
    static boolean inSelector(StackTraceElement element) {
        return element.getClassName().endsWith("SelectorImpl")
                && element.getMethodName().contains("elect");
    }

    /** {@link TcpPassage}가 쓰기 자리를 기다리는 프레임인가. */
    static boolean inWriteWait(StackTraceElement element) {
        return element.getClassName().equals(TcpPassage.class.getName())
                && element.getMethodName().equals("awaitWritable");
    }
}
