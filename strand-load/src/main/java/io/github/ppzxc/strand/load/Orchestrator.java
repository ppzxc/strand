package io.github.ppzxc.strand.load;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
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

/**
 * {@code run} 명령의 진행자. 서버 프로세스와 부하 생성기 프로세스를 따로 띄워 한 번 측정하고 결과 JSON을 낸다. 어느 쪽이든 비정상으로 끝나면 실행 전체를 실패로 처리하고 부분 결과는 남기지
 * 않으며, 끝날 때는 두 프로세스를 반드시 정리한다.
 */
final class Orchestrator {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final Duration STARTUP_LIMIT = Duration.ofSeconds(60);

    /** 실행 실패. 종료 상태 1로 끝난다. */
    static final class RunFailed extends Exception {
        private static final long serialVersionUID = 1L;

        RunFailed(String message) {
            super(message);
        }
    }

    private final RunConfig config;
    private final PrintStream out;

    /** 행렬 실행이 한 번 만든 키 저장소를 여러 실행에 재사용할 때 쓴다. null이면 실행마다 만든다. */
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

    /** 측정하고 결과를 {@code --output}(없으면 표준 출력)에 쓴다. */
    void run() throws RunFailed, IOException, InterruptedException {
        emit(execute());
    }

    /** 측정하고 결과 JSON을 돌려준다. 프로세스는 모두 정리한다. */
    JsonObject execute() throws RunFailed, IOException, InterruptedException {
        try {
            if (config.tls) {
                if (presetKeystore != null) {
                    keystorePath = presetKeystore;
                } else {
                    keystoreDir = Files.createTempDirectory("strand-load-tls");
                    keystorePath = LoadTls.createKeystore(keystoreDir).toString();
                }
            }
            if (config.jfr || config.asyncProfiler != null) {
                Files.createDirectories(Paths.get(config.profileDir));
            }
            return runAll();
        } finally {
            cleanupKeystore();
        }
    }

    private void cleanupKeystore() throws IOException {
        if (keystoreDir != null) {
            try (var files = Files.walk(keystoreDir)) {
                files.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> p.toFile().delete());
            }
        }
    }

    /** 쓰기 정체 시나리오를 돌려 결과를 {@code --output}에 쓴다. */
    void runStall() throws RunFailed, IOException, InterruptedException {
        emit(executeStall());
    }

    private JsonObject runAll() throws RunFailed, IOException, InterruptedException {
        double rate = config.rate;
        Double saturation = null;
        if (config.openLoop && config.rateFraction > 0) {
            if (config.saturation > 0) {
                // 같은 셀에서 이미 잰 포화 처리량을 쓴다(행렬 실행).
                saturation = config.saturation;
            } else {
                // 포화 처리량은 같은 설정을 폐쇄형으로 한 번 돌려 얻는다. 도착률은 그 비율이다.
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

    /**
     * 쓰기 정체 시나리오. 서버가 밀어 넣는 요청을 상대(부하 생성기 프로세스)가 읽지 않게 멈춘 뒤, 서버 메모리 증가와 연결이 끊기기까지의 시간·원인을 잰다. 서버는 기존 유휴 PING·확인 대기로만 끊어야 한다(ADR
     * 0004). 시간 안에 끊기지 않으면 실패다.
     */
    JsonObject executeStall() throws RunFailed, IOException, InterruptedException {
        try {
            if (config.tls) {
                if (presetKeystore != null) {
                    keystorePath = presetKeystore;
                } else {
                    keystoreDir = Files.createTempDirectory("strand-load-tls");
                    keystorePath = LoadTls.createKeystore(keystoreDir).toString();
                }
            }
            return stallOnce();
        } finally {
            cleanupKeystore();
        }
    }

    private JsonObject stallOnce() throws RunFailed, IOException, InterruptedException {
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
            if (peer != null) {
                stopProcess(peer, "CONT");
            }
            terminate(peer);
            terminate(server);
        }
    }

    private JsonObject stallMetadata() {
        JsonObject config = new JsonObject();
        config.addProperty("transport", this.config.transport);
        config.addProperty("tls", this.config.tls);
        if (!this.config.tls) {
            config.addProperty("plaintextControl", true);
        }
        JsonObject metadata = new JsonObject();
        metadata.addProperty("jdk", System.getProperty("java.vm.name") + " " + System.getProperty("java.version"));
        metadata.addProperty("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        metadata.addProperty("cores", Runtime.getRuntime().availableProcessors());
        metadata.addProperty("gitSha", gitSha());
        metadata.add("config", config);
        return metadata;
    }

    /** 프로세스에 SIGSTOP/SIGCONT를 보낸다. 멈춘 프로세스는 소켓을 읽지 않아 상대의 쓰기가 막힌다. */
    private static void stopProcess(Process process, String signal) throws RunFailed, InterruptedException {
        try {
            Process kill = new ProcessBuilder("kill", "-" + signal, String.valueOf(process.pid()))
                    .redirectErrorStream(true)
                    .start();
            if (!kill.waitFor(10, TimeUnit.SECONDS) || kill.exitValue() != 0) {
                throw new RunFailed("kill -" + signal + " 실패: 쓰기 정체 시나리오는 kill 명령이 있는 Linux/macOS가 필요하다");
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

    /** 서버가 알린 마지막 {@code CLOSED} 줄의 JSON. 없으면 null. */
    private String lastClosed;

    /** 서버와 부하 생성기를 새로 띄워 한 번 측정한다. 끝나면 두 프로세스를 정리한다. */
    private JsonObject runOnce(double rate, boolean closedOverride)
            throws RunFailed, IOException, InterruptedException {
        Process server = null;
        Process loadgen = null;
        try {
            List<String> args = config.childArgs(rate, closedOverride, keystorePath);
            server = start(List.of("server"), args);
            StderrCapture serverErr = new StderrCapture(server.getErrorStream());
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
            return runJson(
                    JsonParser.parseString(outcome.resultLine).getAsJsonObject(),
                    outcome.serverResources,
                    server.pid(),
                    loadgen.pid());
        } finally {
            terminate(loadgen);
            terminate(server);
        }
    }

    private static JsonObject runJson(JsonObject raw, JsonObject serverResources, long serverPid, long loadgenPid) {
        double seconds = raw.get("measureNanos").getAsLong() / 1e9;
        long responses = raw.get("responses").getAsLong();
        JsonObject measurement = new JsonObject();
        measurement.addProperty("requests", raw.get("requests").getAsLong());
        measurement.addProperty("responses", responses);
        measurement.addProperty("errors", raw.get("errors").getAsLong());
        measurement.addProperty("unknown", raw.get("unknown").getAsLong());
        measurement.addProperty("measureSeconds", seconds);
        measurement.addProperty("throughputPerSecond", responses / seconds);
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
        if (config.jfr) {
            // JFR 시작 안내가 표준 출력을 어지럽히지 않게 JVM 로그는 끈다.
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

    /**
     * 부하 생성기의 출력을 읽는다. {@code PHASE MEASURE_START/END} 줄이 오면 서버에 표본을 요청해 같은 구간의 서버 자원 사용을 얻고, 마지막 JSON 줄이 부하 생성기의 결과다.
     */
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
        while (System.nanoTime() < limit) {
            String line = loadgenLines.poll(50);
            if (!server.isAlive()) {
                throw new RunFailed("서버 프로세스가 비정상 종료했다(종료 코드 " + exitCode(server) + "): " + serverErr.text());
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
            } else if (line.equals("PHASE MEASURE_END")) {
                serverEnd = sampleServer(server, serverLines, serverErr);
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

    /** 자식 프로세스의 표준 출력을 줄 단위로 모은다. 끝나면 {@link #EOF}를 넣는다. */
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
                    // 프로세스가 끝났다.
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
        // 서버 전송이 유일한 변수다. 부하 생성기는 항상 nio 클라이언트 코드 경로를 쓴다.
        config.addProperty("transport", this.config.transport);
        config.addProperty("loadgenTransport", "nio");
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
            // 평문은 하네스 한정 대조군이다. 순수 전송 오버헤드를 분리하는 데만 쓰며 제품 설정으로 노출되지 않는다.
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

        JsonObject metadata = new JsonObject();
        metadata.addProperty("jdk", System.getProperty("java.vm.name") + " " + System.getProperty("java.version"));
        metadata.addProperty("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        metadata.addProperty("cores", Runtime.getRuntime().availableProcessors());
        metadata.add("gitSha", new com.google.gson.JsonPrimitive(gitSha()));
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

    /** 반복별 값을 중앙값과 범위(최소·최대)로 요약한다. */
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

    static String gitSha() {
        try {
            Process process = new ProcessBuilder("git", "rev-parse", "HEAD")
                    .redirectErrorStream(true)
                    .start();
            String sha = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).strip();
            return process.waitFor(5, TimeUnit.SECONDS) && process.exitValue() == 0 && !sha.isEmpty() ? sha : "unknown";
        } catch (IOException | InterruptedException e) {
            return "unknown";
        }
    }

    /** 하네스 자체 시험용: 지정한 시간 뒤 서버 프로세스를 강제로 죽인다. */
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
        // 정상 종료는 표준 입력을 닫아 알린다. 안 끝나면 강제로 끝낸다.
        try {
            process.getOutputStream().close();
        } catch (IOException ignored) {
            // 이미 끝난 프로세스다.
        }
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
        }
    }

    private static String exitCode(Process process) {
        return process.isAlive() ? "실행 중" : String.valueOf(process.exitValue());
    }

    private static void sleepQuietly() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 자식 프로세스의 표준 오류를 모아 두었다가 실패 메시지에 붙인다. */
    private static final class StderrCapture {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        StderrCapture(InputStream stream) {
            Thread thread = new Thread(() -> {
                byte[] chunk = new byte[2048];
                try {
                    int n;
                    while ((n = stream.read(chunk)) >= 0) {
                        synchronized (buffer) {
                            buffer.write(chunk, 0, n);
                        }
                    }
                } catch (IOException ignored) {
                    // 프로세스가 끝났다.
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        String text() {
            synchronized (buffer) {
                String text = buffer.toString(StandardCharsets.UTF_8).strip();
                return text.isEmpty() ? "(표준 오류 없음)" : text;
            }
        }
    }
}
