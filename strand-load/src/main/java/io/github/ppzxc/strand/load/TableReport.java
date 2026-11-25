package io.github.ppzxc.strand.load;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Locale;

final class TableReport {
    private TableReport() {}

    static String markdown(JsonObject matrix) {
        StringBuilder table = new StringBuilder();
        table.append(
                "| transport | tls | conns | body(B) | throughput/s median [min–max] | @50% p50/p99/p99.9 µs | @80% p50/p99/p99.9 µs | errors | unknown |\n");
        table.append("|---|---|---|---|---|---|---|---|---|\n");
        for (JsonElement element : matrix.getAsJsonArray("cells")) {
            JsonObject cell = element.getAsJsonObject();
            JsonObject closed = cell.getAsJsonObject("closedLoop");
            JsonObject open = cell.getAsJsonObject("openLoop");
            table.append(String.format(
                    Locale.ROOT,
                    "| %s | %s | %d | %d | %s | %s | %s | %d | %d |%n",
                    cell.get("transport").getAsString(),
                    cell.get("tls").getAsBoolean() ? "on" : "off(control)",
                    cell.get("connections").getAsInt(),
                    cell.get("bodyBytes").getAsInt(),
                    throughput(closed),
                    latency(open.getAsJsonObject("50")),
                    latency(open.getAsJsonObject("80")),
                    cell.get("errors").getAsLong(),
                    cell.get("unknown").getAsLong()));
        }
        return table.toString();
    }

    private static String throughput(JsonObject scenario) {
        JsonObject stats = scenario.getAsJsonObject("summary").getAsJsonObject("throughputPerSecond");
        return String.format(
                Locale.ROOT,
                "%.0f [%.0f–%.0f]",
                stats.get("median").getAsDouble(),
                stats.get("min").getAsDouble(),
                stats.get("max").getAsDouble());
    }

    private static String latency(JsonObject scenario) {
        JsonObject latency = scenario.getAsJsonObject("summary").getAsJsonObject("latencyMicros");
        return String.format(
                Locale.ROOT,
                "%.0f / %.0f / %.0f",
                latency.getAsJsonObject("p50").get("median").getAsDouble(),
                latency.getAsJsonObject("p99").get("median").getAsDouble(),
                latency.getAsJsonObject("p999").get("median").getAsDouble());
    }
}
