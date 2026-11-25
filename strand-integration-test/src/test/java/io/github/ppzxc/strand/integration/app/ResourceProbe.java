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

final class ResourceProbe {
    private static final Path FD = Paths.get("/proc/self/fd");

    private ResourceProbe() {}

    static boolean socketsVisible() {
        return Files.isDirectory(FD);
    }

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

    static Set<String> socketsSince(Set<String> baseline) throws IOException {
        Set<String> now = sockets();
        now.removeAll(baseline);
        return now;
    }

    static Set<Thread> strandThreads() {
        Set<Thread> found = new HashSet<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            // These JVM-wide executors outlive individual test rigs.
            if (thread.isAlive()
                    && thread.getName().startsWith("strand-")
                    && !thread.getName().equals("strand-nio-stage-supervisor")
                    && !thread.getName().equals("strand-nio-stage-completion")) {
                found.add(thread);
            }
        }
        return found;
    }

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
