package io.github.ppzxc.strand.load;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 프로세스 자원 표본. CPU 시간·스레드·힙·GC·할당은 JMX로, RSS는 {@code /proc}에서 읽는다. 측정 구간 시작과 끝에 한 번씩 표본을 떠 차이를 낸다. 구간 안의 정확한 할당과 핫스팟은 JFR 기록({@code
 * --jfr})으로 본다. 여기의 할당량은 표본 시점에 살아 있는 스레드의 합이라 근사값이다.
 */
final class ResourceSampler {
    private static final Path PROC_STATUS = Paths.get("/proc/self/status");

    private ResourceSampler() {}

    /** 이 환경에서 자원 수집이 되는지 확인한다. 안 되면 사유를 돌려주고, 되면 null이다. */
    static String unavailableReason() {
        if (!Files.isReadable(PROC_STATUS)) {
            return "자원 수집(RSS)은 /proc/self/status가 필요하다(Linux). 이 환경에는 없다";
        }
        return null;
    }

    /** 현재 프로세스의 표본(원시 값). */
    static JsonObject sample() throws IOException {
        JsonObject sample = new JsonObject();
        sample.addProperty("nanoTime", System.nanoTime());
        sample.addProperty(
                "cpuNanos",
                ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                        .getProcessCpuTime());
        sample.addProperty("rssBytes", rssBytes());
        com.sun.management.ThreadMXBean threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        sample.addProperty("threads", threads.getThreadCount());
        long allocated = 0;
        for (long bytes : threads.getThreadAllocatedBytes(threads.getAllThreadIds())) {
            allocated += Math.max(0, bytes);
        }
        sample.addProperty("allocatedBytes", allocated);
        sample.addProperty(
                "heapUsedBytes",
                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
        long gcCount = 0;
        long gcTime = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcCount += Math.max(0, gc.getCollectionCount());
            gcTime += Math.max(0, gc.getCollectionTime());
        }
        sample.addProperty("gcCount", gcCount);
        sample.addProperty("gcTimeMillis", gcTime);
        return sample;
    }

    /** 두 표본의 차이로 구간의 자원 사용을 만든다. */
    static JsonObject delta(JsonObject start, JsonObject end) {
        double wallSeconds =
                (end.get("nanoTime").getAsLong() - start.get("nanoTime").getAsLong()) / 1e9;
        double cpuSeconds =
                (end.get("cpuNanos").getAsLong() - start.get("cpuNanos").getAsLong()) / 1e9;
        JsonObject resources = new JsonObject();
        resources.addProperty("cpuSeconds", cpuSeconds);
        resources.addProperty("cpuCores", wallSeconds > 0 ? cpuSeconds / wallSeconds : 0);
        resources.addProperty("rssBytes", end.get("rssBytes").getAsLong());
        resources.addProperty(
                "rssGrowthBytes",
                end.get("rssBytes").getAsLong() - start.get("rssBytes").getAsLong());
        resources.addProperty("threads", end.get("threads").getAsInt());
        resources.addProperty("heapUsedBytes", end.get("heapUsedBytes").getAsLong());
        resources.addProperty(
                "gcCount", end.get("gcCount").getAsLong() - start.get("gcCount").getAsLong());
        resources.addProperty(
                "gcTimeMillis",
                end.get("gcTimeMillis").getAsLong() - start.get("gcTimeMillis").getAsLong());
        resources.addProperty(
                "allocatedBytes",
                Math.max(
                        0,
                        end.get("allocatedBytes").getAsLong()
                                - start.get("allocatedBytes").getAsLong()));
        return resources;
    }

    private static long rssBytes() throws IOException {
        for (String line : Files.readAllLines(PROC_STATUS, StandardCharsets.UTF_8)) {
            if (line.startsWith("VmRSS:")) {
                String digits = line.replaceAll("[^0-9]", "");
                return Long.parseLong(digits) * 1024;
            }
        }
        throw new IOException("/proc/self/status에 VmRSS가 없다");
    }
}
