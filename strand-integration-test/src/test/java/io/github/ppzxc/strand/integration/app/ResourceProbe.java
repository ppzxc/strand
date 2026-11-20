package io.github.ppzxc.strand.integration.app;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.AbstractScheduledEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** 테스트가 남긴 소켓·스레드·타이머를 보는 도구. 소켓은 Linux의 {@code /proc/self/fd}가 있을 때만 본다. */
final class ResourceProbe {
    private static final Path FD = Paths.get("/proc/self/fd");

    private ResourceProbe() {}

    static boolean socketsVisible() {
        return Files.isDirectory(FD);
    }

    /** 이 프로세스가 연 소켓의 식별자(inode) 집합. */
    static Set<String> sockets() throws IOException {
        Set<String> found = new HashSet<>();
        try (DirectoryStream<Path> fds = Files.newDirectoryStream(FD)) {
            for (Path fd : fds) {
                String target;
                try {
                    target = Files.readSymbolicLink(fd).toString();
                } catch (IOException closedMeanwhile) {
                    continue;
                }
                if (target.startsWith("socket:")) {
                    found.add(target);
                }
            }
        }
        return found;
    }

    /** {@code baseline}에 없던 소켓. */
    static Set<String> socketsSince(Set<String> baseline) throws IOException {
        Set<String> now = sockets();
        now.removeAll(baseline);
        return now;
    }

    /** 이름이 {@code strand-}로 시작하는 살아 있는 스레드. */
    static Set<Thread> strandThreads() {
        Set<Thread> found = new HashSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && thread.getName().startsWith("strand-")) {
                found.add(thread);
            }
        }
        return found;
    }

    /** 앱이 준 EventLoopGroup의 어느 EventLoop에 예약된 작업(타이머)이 남아 있는가. 각 EventLoop 안에서 확인한다. */
    static boolean hasScheduledTasks(EventLoopGroup group) throws Exception {
        Method method = AbstractScheduledEventExecutor.class.getDeclaredMethod("hasScheduledTasks");
        method.setAccessible(true);
        for (EventExecutor executor : group) {
            Boolean scheduled =
                    executor.submit(() -> (Boolean) method.invoke(executor)).get(Rig.WAIT_SECONDS, TimeUnit.SECONDS);
            if (scheduled) {
                return true;
            }
        }
        return false;
    }
}
