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

    Orchestrator(RunConfig config, PrintStream out) {
        this.config = config;
        this.out = out;
    }

    void run() throws RunFailed, IOException, InterruptedException {
        Process server = null;
        Process loadgen = null;
        StderrCapture serverErr = null;
        StderrCapture loadgenErr = null;
        try {
            server = start(List.of("server"), config.childArgs());
            serverErr = new StderrCapture(server.getErrorStream());
            int port = awaitReady(server, serverErr);

            if (config.chaosKillServerAfter != null) {
                scheduleKill(server, config.chaosKillServerAfter);
            }

            List<String> loadgenArgs = new ArrayList<>(config.childArgs());
            loadgenArgs.addAll(List.of("--port", String.valueOf(port)));
            loadgen = start(List.of("loadgen"), loadgenArgs);
            loadgenErr = new StderrCapture(loadgen.getErrorStream());
            String resultLine = awaitLoadGen(loadgen, loadgenErr, server, serverErr);

            JsonObject measurement = JsonParser.parseString(resultLine).getAsJsonObject();
            writeResult(measurement, server.pid(), loadgen.pid());
        } finally {
            terminate(loadgen);
            terminate(server);
        }
    }

    private Process start(List<String> role, List<String> args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
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

    private void writeResult(JsonObject measurementRaw, long serverPid, long loadgenPid) throws IOException {
        double seconds = measurementRaw.get("measureNanos").getAsLong() / 1e9;
        long responses = measurementRaw.get("responses").getAsLong();
        JsonObject measurement = new JsonObject();
        measurement.addProperty("requests", measurementRaw.get("requests").getAsLong());
        measurement.addProperty("responses", responses);
        measurement.addProperty("errors", measurementRaw.get("errors").getAsLong());
        measurement.addProperty("unknown", measurementRaw.get("unknown").getAsLong());
        measurement.addProperty("measureSeconds", seconds);
        measurement.addProperty("throughputPerSecond", responses / seconds);

        JsonObject processes = new JsonObject();
        processes.addProperty("server", serverPid);
        processes.addProperty("loadgen", loadgenPid);

        JsonObject config = new JsonObject();
        // 서버 전송이 유일한 변수다. 부하 생성기는 항상 nio 클라이언트 코드 경로를 쓴다.
        config.addProperty("transport", this.config.transport);
        config.addProperty("loadgenTransport", "nio");
        if ("netty".equals(this.config.transport)) {
            config.addProperty("nettyEventLoops", ServerRole.NETTY_EVENT_LOOPS);
        }
        config.addProperty("tls", false);
        config.addProperty("connections", this.config.connections);
        config.addProperty("bodyBytes", this.config.bodyBytes);
        config.addProperty("inflight", this.config.inflight);
        config.addProperty("loadModel", "closed-loop");
        config.addProperty("warmupMillis", this.config.warmup.toMillis());
        config.addProperty("measureMillis", this.config.measure.toMillis());

        JsonObject metadata = new JsonObject();
        metadata.addProperty("jdk", System.getProperty("java.vm.name") + " " + System.getProperty("java.version"));
        metadata.addProperty("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        metadata.addProperty("cores", Runtime.getRuntime().availableProcessors());
        metadata.addProperty("gitSha", gitSha());
        metadata.add("config", config);
        metadata.add("processes", processes);

        JsonObject result = new JsonObject();
        result.addProperty("schemaVersion", 1);
        result.addProperty("status", "OK");
        result.add("metadata", metadata);
        result.add("measurement", measurement);

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

    private static String gitSha() {
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
