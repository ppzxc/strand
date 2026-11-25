package io.github.ppzxc.strand.load;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

final class ResourceSampler {
    private static final Path PROC_STATUS = Paths.get("/proc/self/status");

    private ResourceSampler() {}

    static String unavailableReason() {
        if (!Files.isReadable(PROC_STATUS)) {
            return "자원 수집(RSS)은 /proc/self/status가 필요하다(Linux). 이 환경에는 없다";
        }
        return null;
    }

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
