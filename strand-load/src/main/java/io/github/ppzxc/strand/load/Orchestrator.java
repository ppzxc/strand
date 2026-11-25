package io.github.ppzxc.strand.load;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

final class Orchestrator {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    private static final long UNRESPONSIVE = 6;

    private static final Duration STARTUP_LIMIT = Duration.ofSeconds(60);

    static final class RunFailed extends Exception {
        private static final long serialVersionUID = 1L;

        RunFailed(String message) {
            super(message);
        }
    }

    private final RunConfig config;
    private final PrintStream out;

    private final String presetKeystore;

    Orchestrator(RunConfig config, PrintStream out) {
        this(config, out, null);
    }

    Orchestrator(RunConfig config, PrintStream out, String presetKeystore) {
        this.config = config;
        this.out = out;
        this.presetKeystore = presetKeystore;
    }

    private Path keystoreDir;
    private String keystorePath = "";

    void run() throws RunFailed, IOException, InterruptedException {
        emit(execute());
    }

    JsonObject execute() throws RunFailed, IOException, InterruptedException {
        try {
            prepareKeystore();
            return runAll();
        } finally {
            cleanupKeystore();
        }
    }

    private void prepareKeystore() throws IOException, InterruptedException {
        if (config.jfr || config.asyncProfiler != null) {

            Files.createDirectories(Paths.get(config.profileDir));
        }
        if (!config.tls) {
            return;
        }
        if (presetKeystore != null) {
            keystorePath = presetKeystore;
        } else {
            keystoreDir = Files.createTempDirectory("strand-load-tls");
            keystorePath = LoadTls.createKeystore(keystoreDir).toString();
        }
    }

    private void cleanupKeystore() {
        if (keystoreDir != null) {
            try {
                LoadTls.deleteTree(keystoreDir);
            } catch (IOException ignored) {

            }
        }
    }

    JsonObject executeSoak() throws RunFailed, IOException, InterruptedException {
        try {
            prepareKeystore();
            JsonObject run = runOnce(config.rate, false);
            JsonObject measurement = run.getAsJsonObject("measurement");

            JsonObject series = new JsonObject();
            series.add("server", seriesJson(serverSeries));
            com.google.gson.JsonArray loadgenSamples =
                    lastRaw.has("samples") ? lastRaw.getAsJsonArray("samples") : new com.google.gson.JsonArray();
            List<JsonObject> loadgen = new ArrayList<>();
            loadgenSamples.forEach(e -> loadgen.add(e.getAsJsonObject()));
            series.add("loadgen", seriesJson(loadgen));

            JsonObject trend = new JsonObject();
            trend.add("server", trendJson(serverSeries));
            trend.add("loadgen", trendJson(loadgen));

            JsonObject leaks = new JsonObject();
            lastServerErr.drain();
            leaks.addProperty("count", lastServerErr.leakCount());
            com.google.gson.JsonArray leakLines = new com.google.gson.JsonArray();
            lastServerErr.leakLines().forEach(leakLines::add);
            leaks.add("lines", leakLines);
            leaks.addProperty("detector", "netty ResourceLeakDetector(paranoid), 서버 표준 오류의 LEAK: 줄");

            leaks.addProperty("lowerBound", true);
            leaks.addProperty("applicable", config.transport.equals("netty"));

            JsonObject load = new JsonObject();
            for (String key : new String[] {"requests", "responses", "errors", "unknown", "throughputPerSecond"}) {
                load.add(key, measurement.get(key));
            }
            load.add("latencyMicros", measurement.get("latencyMicros"));

            JsonObject soakConfig = new JsonObject();
            soakConfig.addProperty("transport", config.transport);
            soakConfig.addProperty("tls", config.tls);
            soakConfig.addProperty("connections", config.connections);
            soakConfig.addProperty("bodyBytes", config.bodyBytes);
            soakConfig.addProperty("rateTargetPerSecond", config.rate);
            soakConfig.addProperty("durationMillis", config.measure.toMillis());
            soakConfig.addProperty("sampleIntervalMillis", config.sampleInterval.toMillis());

            JsonObject result = new JsonObject();
            result.addProperty("schemaVersion", 1);
            result.addProperty("kind", "soak");
            result.addProperty("status", "OK");
            result.add("metadata", stallMetadata());
            result.add("config", soakConfig);
            result.add("load", load);
            result.add("series", series);
            result.add("trend", trend);
            result.add("leaks", leaks);
            return result;
        } finally {
            cleanupKeystore();
        }
    }

    void runSoak() throws RunFailed, IOException, InterruptedException {
        emit(executeSoak());
    }

    private static com.google.gson.JsonArray seriesJson(List<JsonObject> samples) {
        com.google.gson.JsonArray array = new com.google.gson.JsonArray();
        if (samples.isEmpty()) {
            return array;
        }
        long t0 = samples.get(0).get("nanoTime").getAsLong();
        for (JsonObject sample : samples) {
            JsonObject point = new JsonObject();
            point.addProperty("tSeconds", (sample.get("nanoTime").getAsLong() - t0) / 1e9);
            point.addProperty("rssBytes", sample.get("rssBytes").getAsLong());
            point.addProperty("threads", sample.get("threads").getAsInt());
            point.addProperty("heapUsedBytes", sample.get("heapUsedBytes").getAsLong());
            point.addProperty("gcCount", sample.get("gcCount").getAsLong());
            point.addProperty("gcTimeMillis", sample.get("gcTimeMillis").getAsLong());
            point.addProperty("cpuSeconds", sample.get("cpuNanos").getAsLong() / 1e9);
            array.add(point);
        }
        return array;
    }

    private static JsonObject trendJson(List<JsonObject> samples) {
        JsonObject trend = new JsonObject();
        if (samples.size() < 2) {
            return trend;
        }
        JsonObject first = samples.get(0);
        JsonObject last = samples.get(samples.size() - 1);
        double hours = (last.get("nanoTime").getAsLong() - first.get("nanoTime").getAsLong()) / 3.6e12;
        long rssGrowth =
                last.get("rssBytes").getAsLong() - first.get("rssBytes").getAsLong();
        trend.addProperty("rssGrowthBytes", rssGrowth);
        trend.addProperty("rssGrowthBytesPerHour", hours > 0 ? rssGrowth / hours : 0);
        trend.addProperty(
                "threadGrowth",
                last.get("threads").getAsInt() - first.get("threads").getAsInt());
        trend.addProperty(
                "gcCount",
                last.get("gcCount").getAsLong() - first.get("gcCount").getAsLong());
        return trend;
    }

    void runStall() throws RunFailed, IOException, InterruptedException {
        emit(executeStall());
    }

    private JsonObject runAll() throws RunFailed, IOException, InterruptedException {
        double rate = config.rate;
        Double saturation = null;
        if (config.openLoop && config.rateFraction > 0) {
            if (config.saturation > 0) {

                saturation = config.saturation;
            } else {

                JsonObject saturationRun = runOnce(0, true);
                saturation = saturationRun
                        .getAsJsonObject("measurement")
                        .get("throughputPerSecond")
                        .getAsDouble();
            }
            rate = saturation * config.rateFraction;
        }
        List<JsonObject> runs = new ArrayList<>();
        for (int i = 0; i < config.repetitions; i++) {
            runs.add(runOnce(rate, false));
        }
        return buildResult(runs, saturation, rate);
    }

    JsonObject executeStall() throws RunFailed, IOException, InterruptedException {
        try {
            prepareKeystore();
            return stallOnce();
        } finally {
            cleanupKeystore();
        }
    }

    private JsonObject stallOnce() throws RunFailed, IOException, InterruptedException {
        lastClosed = null;
        Process server = null;
        Process peer = null;
        try {
            List<String> args = config.childArgs(0, true, keystorePath);
            server = start(List.of("server"), args);
            StderrCapture serverErr = new StderrCapture(server.getErrorStream());
            Lines serverLines = new Lines(server.getInputStream());
            int port = awaitReady(server, serverLines, serverErr);

            List<String> peerArgs = new ArrayList<>(args);
            peerArgs.addAll(List.of("--port", String.valueOf(port)));
            peer = start(List.of("loadgen"), peerArgs);
            StderrCapture peerErr = new StderrCapture(peer.getErrorStream());
            Lines peerLines = new Lines(peer.getInputStream());
            awaitLine(peerLines, "PEER_READY", peer, peerErr, Duration.ofSeconds(60), "상대");

            long baseline =
                    sampleServer(server, serverLines, serverErr).get("rssBytes").getAsLong();
            long peak = baseline;
            if (lastClosed != null) {
                throw new RunFailed("상대를 멈추기 전에 이미 연결이 끊겼다: " + lastClosed);
            }
            stopProcess(peer, "STOP");
            long stoppedAt = System.nanoTime();
            long deadline = stoppedAt + config.maxWait.toNanos();
            String closedJson = null;
            long detectedAt = 0;
            long end = baseline;
            while (System.nanoTime() < deadline) {
                end = sampleServer(server, serverLines, serverErr)
                        .get("rssBytes")
                        .getAsLong();
                peak = Math.max(peak, end);
                if (lastClosed != null) {
                    closedJson = lastClosed;
                    detectedAt = System.nanoTime();
                    JsonObject notice = JsonParser.parseString(closedJson).getAsJsonObject();

                    if (!notice.get("kind").getAsString().equals("ERROR_CLOSE")
                            || notice.get("errorCode").getAsLong() != UNRESPONSIVE) {
                        stopProcess(peer, "CONT");
                        throw new RunFailed("연결이 예상한 확인 대기 초과(ERROR_CLOSE, 오류 코드 " + UNRESPONSIVE
                                + ")가 아닌 다른 원인으로 끊겼다: " + closedJson);
                    }
                    break;
                }
                Thread.sleep(250);
            }
            stopProcess(peer, "CONT");
            if (closedJson == null) {
                throw new RunFailed("쓰기가 막힌 연결이 " + config.maxWait.toMillis()
                        + "ms 안에 감지되어 끊기지 않았다(유휴 PING 간격 " + config.idlePing.toMillis() + "ms, 확인 대기 "
                        + config.ackTimeout.toMillis() + "ms). 서버 RSS 증가 "
                        + (peak - baseline) + "바이트");
            }

            JsonObject stall = new JsonObject();
            stall.addProperty("detected", true);
            stall.addProperty("detectionMillis", Math.max(1, (detectedAt - stoppedAt) / 1_000_000));
            stall.add("close", JsonParser.parseString(closedJson).getAsJsonObject());
            JsonObject memory = new JsonObject();
            memory.addProperty("baselineRssBytes", baseline);
            memory.addProperty("peakRssBytes", peak);
            memory.addProperty("peakGrowthBytes", peak - baseline);
            memory.addProperty("endRssBytes", end);
            stall.add("memory", memory);
            stall.addProperty("idlePingMillis", config.idlePing.toMillis());
            stall.addProperty("ackTimeoutMillis", config.ackTimeout.toMillis());
            stall.addProperty("maxWaitMillis", config.maxWait.toMillis());
            stall.addProperty("bodyBytes", config.bodyBytes);

            JsonObject result = new JsonObject();
            result.addProperty("schemaVersion", 1);
            result.addProperty("kind", "write-stall");
            result.addProperty("status", "OK");
            result.add("metadata", stallMetadata());
            result.add("stall", stall);
            return result;
        } finally {
            try {
                if (peer != null) {
                    stopProcess(peer, "CONT");
                }
            } catch (RunFailed ignored) {

            } finally {
                try {
                    terminate(peer);
                } finally {
                    terminate(server);
                }
            }
        }
    }

    private JsonObject stallMetadata() {
        JsonObject config = new JsonObject();
        config.addProperty("transport", this.config.transport);
        config.addProperty("tls", this.config.tls);
        if (!this.config.tls) {
            config.addProperty("plaintextControl", true);
        }
        JsonObject metadata = environmentMetadata();
        metadata.add("config", config);
        return metadata;
    }

    private static void stopProcess(Process process, String signal) throws RunFailed, InterruptedException {
        try {
            Process kill = new ProcessBuilder("kill", "-" + signal, String.valueOf(process.pid()))
                    .redirectErrorStream(true)
                    .start();
            if (!kill.waitFor(10, TimeUnit.SECONDS) || kill.exitValue() != 0) {
                throw new RunFailed("kill -" + signal + " 실패: 쓰기 정체 시나리오는 kill 명령이 있는 Linux가 필요하다");
            }
        } catch (IOException e) {
            throw new RunFailed("kill 명령을 실행할 수 없다: 쓰기 정체 시나리오는 kill 명령이 필요하다(" + e.getMessage() + ")");
        }
    }

    private void awaitLine(Lines lines, String expected, Process process, StderrCapture err, Duration limit, String who)
            throws RunFailed, InterruptedException {
        long end = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < end) {
            String line = lines.poll(100);
            if (line == null) {
                continue;
            }
            if (line.equals(expected)) {
                return;
            }
            if (line.equals(Lines.EOF)) {
                break;
            }
        }
        throw new RunFailed(who + " 프로세스가 " + expected + "를 알리지 않았다(종료 코드 " + exitCode(process) + "): " + err.text());
    }

    private final List<JsonObject> serverSeries = new ArrayList<>();
    private StderrCapture lastServerErr;
    private JsonObject lastRaw;

    private String lastClosed;

    private JsonObject runOnce(double rate, boolean closedOverride)
            throws RunFailed, IOException, InterruptedException {
        serverSeries.clear();
        Process server = null;
        Process loadgen = null;
        try {
            List<String> args = config.childArgs(rate, closedOverride, keystorePath);
            server = start(List.of("server"), args);
            StderrCapture serverErr = new StderrCapture(server.getErrorStream());
            lastServerErr = serverErr;
            Lines serverLines = new Lines(server.getInputStream());
            int port = awaitReady(server, serverLines, serverErr);

            if (config.chaosKillServerAfter != null) {
                scheduleKill(server, config.chaosKillServerAfter);
            }

            List<String> loadgenArgs = new ArrayList<>(args);
            loadgenArgs.addAll(List.of("--port", String.valueOf(port)));
            loadgen = start(List.of("loadgen"), loadgenArgs);
            StderrCapture loadgenErr = new StderrCapture(loadgen.getErrorStream());
            Lines loadgenLines = new Lines(loadgen.getInputStream());
            LoadGenOutcome outcome = awaitLoadGen(loadgen, loadgenLines, loadgenErr, server, serverLines, serverErr);
            lastRaw = JsonParser.parseString(outcome.resultLine).getAsJsonObject();
            return runJson(lastRaw, outcome.serverResources, server.pid(), loadgen.pid());
        } finally {
            terminate(loadgen);
            terminate(server);
        }
    }

    private static JsonObject runJson(JsonObject raw, JsonObject serverResources, long serverPid, long loadgenPid)
            throws RunFailed {
        long measureNanos = raw.get("measureNanos").getAsLong();
        if (measureNanos <= 0) {
            throw new RunFailed("부하 생성기가 측정 구간이 시작되기 전에 도착률 일정을 지키지 못해 포기했다. 도착률을 낮추거나 코어를 나눠 다시 잰다");
        }
        double seconds = measureNanos / 1e9;
        long responses = raw.get("responses").getAsLong();
        long windowResponses = raw.get("windowResponses").getAsLong();
        JsonObject measurement = new JsonObject();
        measurement.addProperty("requests", raw.get("requests").getAsLong());
        measurement.addProperty("responses", responses);
        measurement.addProperty("errors", raw.get("errors").getAsLong());
        measurement.addProperty("unknown", raw.get("unknown").getAsLong());
        measurement.addProperty("measureSeconds", seconds);

        measurement.addProperty("throughputPerSecond", windowResponses / seconds);
        measurement.add("latencyMicros", raw.get("latencyMicros"));
        measurement.add("latencyBasis", raw.get("latencyBasis"));
        measurement.add("sendLagP99Micros", raw.get("sendLagP99Micros"));
        measurement.add("loadgenBottleneck", raw.get("loadgenBottleneck"));
        if (raw.has("tls")) {
            measurement.add("negotiatedTls", raw.get("tls"));
        }
        JsonObject resources = new JsonObject();
        resources.add("server", serverResources);
        resources.add("loadgen", raw.get("resources"));
        measurement.add("resources", resources);
        JsonObject processes = new JsonObject();
        processes.addProperty("server", serverPid);
        processes.addProperty("loadgen", loadgenPid);
        JsonObject run = new JsonObject();
        run.add("measurement", measurement);
        run.add("processes", processes);
        return run;
    }

    private Process start(List<String> role, List<String> args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
        if (config.tls) {
            command.addAll(LoadTls.jvmOptions());
        }
        String roleName = role.get(0);
        if (config.soak && roleName.equals("server")) {

            command.add("-Dio.netty.leakDetection.level=paranoid");
        }
        if (config.jfr) {

            command.add("-Xlog:disable");
            command.add("-XX:StartFlightRecording=filename=" + Paths.get(config.profileDir, roleName + "-%p.jfr")
                    + ",dumponexit=true,settings=profile");
        }
        if (config.asyncProfiler != null) {
            command.add("-agentpath:" + config.asyncProfiler + "=start,event=cpu,file="
                    + Paths.get(config.profileDir, roleName + "-%p.html"));
        }
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(LoadCli.class.getName());
        command.addAll(role);
        command.addAll(args);
        return new ProcessBuilder(command)
                .redirectInput(ProcessBuilder.Redirect.PIPE)
                .start();
    }

    private int awaitReady(Process server, Lines serverLines, StderrCapture serverErr)
            throws RunFailed, InterruptedException {
        long limit = System.nanoTime() + STARTUP_LIMIT.toNanos();
        while (System.nanoTime() < limit) {
            String line = serverLines.poll(100);
            if (line == null) {
                continue;
            }
            if (line.equals(Lines.EOF)) {
                break;
            }
            if (line.startsWith("READY ")) {
                return Integer.parseInt(line.substring("READY ".length()).trim());
            }
        }
        throw new RunFailed("서버 프로세스가 준비되지 않았다(종료 코드 " + exitCode(server) + "): " + serverErr.text());
    }

    private static final class LoadGenOutcome {
        String resultLine;
        JsonObject serverResources;
    }

    private LoadGenOutcome awaitLoadGen(
            Process loadgen,
            Lines loadgenLines,
            StderrCapture loadgenErr,
            Process server,
            Lines serverLines,
            StderrCapture serverErr)
            throws RunFailed, IOException, InterruptedException {
        long limit = System.nanoTime()
                + config.warmup
                        .plus(config.measure)
                        .plus(Duration.ofSeconds(120))
                        .toNanos();
        JsonObject serverStart = null;
        JsonObject serverEnd = null;
        String resultLine = null;
        long nextSoakSample = System.nanoTime();
        while (System.nanoTime() < limit) {
            String line = loadgenLines.poll(50);
            if (!server.isAlive()) {
                throw new RunFailed("서버 프로세스가 비정상 종료했다(종료 코드 " + exitCode(server) + "): " + serverErr.text());
            }
            if (config.soak && serverStart != null && serverEnd == null && System.nanoTime() >= nextSoakSample) {
                serverSeries.add(sampleServer(server, serverLines, serverErr));
                nextSoakSample = System.nanoTime() + config.sampleInterval.toNanos();
            }
            if (line == null) {
                continue;
            }
            if (line.equals(Lines.EOF)) {
                loadgen.waitFor(10, TimeUnit.SECONDS);
                if (loadgen.isAlive() || loadgen.exitValue() != 0) {
                    throw new RunFailed("부하 생성기 프로세스가 비정상 종료했다(종료 코드 " + exitCode(loadgen) + "): " + loadgenErr.text());
                }
                if (resultLine == null || serverStart == null || serverEnd == null) {
                    throw new RunFailed("부하 생성기가 결과를 내지 않았다: " + loadgenErr.text());
                }
                LoadGenOutcome outcome = new LoadGenOutcome();
                outcome.resultLine = resultLine;
                outcome.serverResources = ResourceSampler.delta(serverStart, serverEnd);
                return outcome;
            }
            if (line.equals("PHASE MEASURE_START")) {
                serverStart = sampleServer(server, serverLines, serverErr);
                serverSeries.add(serverStart);
                nextSoakSample = System.nanoTime() + config.sampleInterval.toNanos();
            } else if (line.equals("PHASE MEASURE_END")) {
                serverEnd = sampleServer(server, serverLines, serverErr);
                serverSeries.add(serverEnd);
            } else if (line.startsWith("{")) {
                resultLine = line;
            }
        }
        throw new RunFailed("부하 생성기가 제한 시간 안에 끝나지 않았다");
    }

    private JsonObject sampleServer(Process server, Lines serverLines, StderrCapture serverErr)
            throws RunFailed, InterruptedException {
        try {
            server.getOutputStream().write("SAMPLE\n".getBytes(StandardCharsets.UTF_8));
            server.getOutputStream().flush();
        } catch (IOException e) {
            throw new RunFailed("서버 프로세스가 비정상 종료했다(종료 코드 " + exitCode(server) + "): " + serverErr.text());
        }
        long limit = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < limit) {
            String line = serverLines.poll(100);
            if (line == null) {
                continue;
            }
            if (line.equals(Lines.EOF)) {
                break;
            }
            if (line.startsWith("CLOSED ")) {
                lastClosed = line.substring("CLOSED ".length());
            }
            if (line.startsWith("SAMPLE ")) {
                return JsonParser.parseString(line.substring("SAMPLE ".length()))
                        .getAsJsonObject();
            }
        }
        throw new RunFailed("서버 프로세스가 자원 표본에 답하지 않았다(종료 코드 " + exitCode(server) + "): " + serverErr.text());
    }

    private static final class Lines {
        static final String EOF = "\u0000EOF";
        private final java.util.concurrent.BlockingQueue<String> queue =
                new java.util.concurrent.LinkedBlockingQueue<>();

        Lines(InputStream stream) {
            Thread thread = new Thread(() -> {
                try (BufferedReader reader =
                        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        queue.add(line);
                    }
                } catch (IOException ignored) {

                }
                queue.add(EOF);
            });
            thread.setDaemon(true);
            thread.start();
        }

        String poll(long millis) throws InterruptedException {
            return queue.poll(millis, TimeUnit.MILLISECONDS);
        }
    }

    private JsonObject buildResult(List<JsonObject> runs, Double saturation, double rate) {
        JsonObject config = new JsonObject();

        config.addProperty("transport", this.config.transport);
        config.addProperty("loadgenTransport", this.config.loadgenTransport);
        config.addProperty("binding", this.config.binding);
        if ("netty".equals(this.config.transport)) {
            config.addProperty("nettyEventLoops", ServerRole.NETTY_EVENT_LOOPS);
        }
        config.addProperty("tls", this.config.tls);
        if (this.config.tls) {
            JsonObject policy = new JsonObject();
            policy.addProperty("protocol", LoadTls.PROTOCOL);
            policy.addProperty("cipherSuite", LoadTls.CIPHER_SUITE);
            policy.addProperty("provider", "JDK default");
            config.add("tlsPolicy", policy);
        } else {

            config.addProperty("plaintextControl", true);
        }
        config.addProperty("connections", this.config.connections);
        config.addProperty("bodyBytes", this.config.bodyBytes);
        config.addProperty("inflight", this.config.effectiveInflight());
        config.addProperty("loadModel", this.config.openLoop ? "open-loop" : "closed-loop");
        if (this.config.openLoop) {
            config.addProperty("rateTargetPerSecond", rate);
            if (saturation != null) {
                config.addProperty("saturationThroughputPerSecond", saturation);
                config.addProperty("rateFractionOfSaturation", this.config.rateFraction);
            }
        }
        config.addProperty("warmupMillis", this.config.warmup.toMillis());
        config.addProperty("measureMillis", this.config.measure.toMillis());
        config.addProperty("repetitions", this.config.repetitions);
        if (!this.config.serverDelay.isZero()) {
            config.addProperty("serverDelayMillis", this.config.serverDelay.toMillis());
        }

        JsonObject metadata = environmentMetadata();
        metadata.add("config", config);

        com.google.gson.JsonArray runArray = new com.google.gson.JsonArray();
        runs.forEach(runArray::add);

        JsonObject result = new JsonObject();
        result.addProperty("schemaVersion", 1);
        result.addProperty("status", "OK");
        result.add("metadata", metadata);
        result.add("runs", runArray);
        result.add("summary", summary(runs));

        return result;
    }

    private void emit(JsonObject result) throws IOException {
        String json = GSON.toJson(result);
        if (this.config.output == null) {
            out.println(json);
        } else {
            Path path = Paths.get(this.config.output);
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(path, json + System.lineSeparator());
        }
    }

    private static JsonObject summary(List<JsonObject> runs) {
        JsonObject summary = new JsonObject();
        summary.add(
                "throughputPerSecond",
                stats(runs, m -> m.get("throughputPerSecond").getAsDouble()));
        JsonObject latency = new JsonObject();
        for (String percentile : new String[] {"p50", "p99", "p999", "max"}) {
            latency.add(
                    percentile,
                    stats(
                            runs,
                            m -> m.getAsJsonObject("latencyMicros")
                                    .get(percentile)
                                    .getAsDouble()));
        }
        summary.add("latencyMicros", latency);
        return summary;
    }

    private static JsonObject stats(List<JsonObject> runs, java.util.function.ToDoubleFunction<JsonObject> value) {
        double[] values = runs.stream()
                .mapToDouble(run -> value.applyAsDouble(run.getAsJsonObject("measurement")))
                .sorted()
                .toArray();
        int n = values.length;
        double median = n % 2 == 1 ? values[n / 2] : (values[n / 2 - 1] + values[n / 2]) / 2.0;
        JsonObject stats = new JsonObject();
        stats.addProperty("median", median);
        stats.addProperty("min", values[0]);
        stats.addProperty("max", values[n - 1]);
        return stats;
    }

    static JsonObject environmentMetadata() {
        JsonObject metadata = new JsonObject();
        metadata.addProperty("jdk", System.getProperty("java.vm.name") + " " + System.getProperty("java.version"));
        metadata.addProperty("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        metadata.addProperty("cores", Runtime.getRuntime().availableProcessors());
        metadata.addProperty("gitSha", gitSha());
        return metadata;
    }

    private static String cachedGitSha;

    static synchronized String gitSha() {
        if (cachedGitSha == null) {
            cachedGitSha = readGitSha();
        }
        return cachedGitSha;
    }

    private static String readGitSha() {
        try {
            Process process = new ProcessBuilder("git", "rev-parse", "HEAD")
                    .redirectErrorStream(true)
                    .start();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                return "unknown";
            }
            String sha = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
            return process.exitValue() == 0 && !sha.isEmpty() ? sha : "unknown";
        } catch (IOException | InterruptedException e) {
            return "unknown";
        }
    }

    private static void scheduleKill(Process server, Duration after) {
        Thread killer = new Thread(() -> {
            try {
                Thread.sleep(after.toMillis());
            } catch (InterruptedException e) {
                return;
            }
            server.destroyForcibly();
        });
        killer.setDaemon(true);
        killer.start();
    }

    private static void terminate(Process process) throws InterruptedException {
        if (process == null) {
            return;
        }

        try {
            process.getOutputStream().close();
        } catch (IOException ignored) {

        }
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
        }
    }

    private static String exitCode(Process process) {
        return process.isAlive() ? "실행 중" : String.valueOf(process.exitValue());
    }

    static final class StderrCapture {
        private static final int MAX_LOG_LINES = 20;
        private static final int MAX_LINE_CHARS = 2_048;
        private static final int MAX_LOG_CHARS = 16_384;
        private final java.util.ArrayDeque<String> lines = new java.util.ArrayDeque<>();
        private final java.util.ArrayDeque<String> leakLines = new java.util.ArrayDeque<>();
        private long leakCount;

        private final Thread reader;

        StderrCapture(InputStream stream) {
            Thread thread = new Thread(() -> {
                StringBuilder line = new StringBuilder();
                StringBuilder marker = new StringBuilder();
                boolean leak = false;
                try {
                    java.io.Reader input = new java.io.InputStreamReader(stream, StandardCharsets.UTF_8);
                    int ch;
                    while ((ch = input.read()) >= 0) {
                        if (ch == '\n') {
                            record(line, leak);
                            line.setLength(0);
                            marker.setLength(0);
                            leak = false;
                            continue;
                        }
                        char value = (char) ch;
                        if (line.length() < MAX_LINE_CHARS) line.append(value);
                        if (marker.length() == 5) marker.deleteCharAt(0);
                        marker.append(value);
                        leak |= marker.indexOf("LEAK:") >= 0;
                    }
                    if (!line.isEmpty() || leak) record(line, leak);
                } catch (IOException ignored) {

                }
            });
            thread.setDaemon(true);
            thread.start();
            this.reader = thread;
        }

        void drain() throws InterruptedException {
            reader.join(2_000);
        }

        private synchronized void record(StringBuilder line, boolean leak) {
            String text = line.toString();
            if (lines.size() == MAX_LOG_LINES) lines.removeFirst();
            lines.addLast(text);
            if (leak) {
                leakCount++;
                if (leakLines.size() == MAX_LOG_LINES) leakLines.removeFirst();
                leakLines.addLast(text);
            }
        }

        synchronized long leakCount() {
            return leakCount;
        }

        synchronized java.util.List<String> leakLines() {
            return java.util.List.copyOf(leakLines);
        }

        synchronized String text() {
            String text = String.join("\n", lines);
            if (text.length() > MAX_LOG_CHARS) text = text.substring(text.length() - MAX_LOG_CHARS);
            return text.isEmpty() ? "(표준 오류 없음)" : text;
        }
    }
}
