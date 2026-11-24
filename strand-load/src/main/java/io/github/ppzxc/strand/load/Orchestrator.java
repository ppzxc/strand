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
            return runAll();
        } finally {
            if (keystoreDir != null) {
                try (var files = Files.walk(keystoreDir)) {
                    files.sorted(java.util.Comparator.reverseOrder())
                            .forEach(p -> p.toFile().delete());
                }
            }
        }
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

    /** 서버와 부하 생성기를 새로 띄워 한 번 측정한다. 끝나면 두 프로세스를 정리한다. */
    private JsonObject runOnce(double rate, boolean closedOverride)
            throws RunFailed, IOException, InterruptedException {
        Process server = null;
        Process loadgen = null;
        try {
            List<String> args = config.childArgs(rate, closedOverride, keystorePath);
            server = start(List.of("server"), args);
            StderrCapture serverErr = new StderrCapture(server.getErrorStream());
            int port = awaitReady(server, serverErr);

            if (config.chaosKillServerAfter != null) {
                scheduleKill(server, config.chaosKillServerAfter);
            }

            List<String> loadgenArgs = new ArrayList<>(args);
            loadgenArgs.addAll(List.of("--port", String.valueOf(port)));
            loadgen = start(List.of("loadgen"), loadgenArgs);
            StderrCapture loadgenErr = new StderrCapture(loadgen.getErrorStream());
            String resultLine = awaitLoadGen(loadgen, loadgenErr, server, serverErr);
            return runJson(JsonParser.parseString(resultLine).getAsJsonObject(), server.pid(), loadgen.pid());
        } finally {
            terminate(loadgen);
            terminate(server);
        }
    }

    private static JsonObject runJson(JsonObject raw, long serverPid, long loadgenPid) {
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
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(LoadCli.class.getName());
        command.addAll(role);
        command.addAll(args);
        return new ProcessBuilder(command)
                .redirectInput(ProcessBuilder.Redirect.PIPE)
                .start();
    }

    private int awaitReady(Process server, StderrCapture serverErr) throws RunFailed, IOException {
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(server.getInputStream(), StandardCharsets.UTF_8));
        long limit = System.nanoTime() + STARTUP_LIMIT.toNanos();
        while (System.nanoTime() < limit) {
            if (reader.ready()) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                if (line.startsWith("READY ")) {
                    return Integer.parseInt(line.substring("READY ".length()).trim());
                }
            } else if (!server.isAlive()) {
                break;
            } else {
                sleepQuietly();
            }
        }
        throw new RunFailed("서버 프로세스가 준비되지 않았다(종료 코드 " + exitCode(server) + "): " + serverErr.text());
    }

    private String awaitLoadGen(Process loadgen, StderrCapture loadgenErr, Process server, StderrCapture serverErr)
            throws RunFailed, IOException {
        long limit = System.nanoTime()
                + config.warmup
                        .plus(config.measure)
                        .plus(Duration.ofSeconds(120))
                        .toNanos();
        ByteArrayOutputStream collected = new ByteArrayOutputStream();
        InputStream in = loadgen.getInputStream();
        byte[] buffer = new byte[4096];
        while (System.nanoTime() < limit) {
            while (in.available() > 0) {
                int n = in.read(buffer);
                if (n < 0) {
                    break;
                }
                collected.write(buffer, 0, n);
            }
            if (!server.isAlive()) {
                throw new RunFailed("서버 프로세스가 비정상 종료했다(종료 코드 " + exitCode(server) + "): " + serverErr.text());
            }
            if (!loadgen.isAlive()) {
                in.transferTo(collected);
                if (loadgen.exitValue() != 0) {
                    throw new RunFailed(
                            "부하 생성기 프로세스가 비정상 종료했다(종료 코드 " + loadgen.exitValue() + "): " + loadgenErr.text());
                }
                String text = collected.toString(StandardCharsets.UTF_8).strip();
                String[] lines = text.split("\\R");
                if (text.isEmpty()) {
                    throw new RunFailed("부하 생성기가 결과를 내지 않았다: " + loadgenErr.text());
                }
                return lines[lines.length - 1];
            }
            sleepQuietly();
        }
        throw new RunFailed("부하 생성기가 제한 시간 안에 끝나지 않았다");
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
