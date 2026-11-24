package io.github.ppzxc.strand.load;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** 하네스 CLI가 유일한 seam이다. 결과 JSON과 종료 상태만 검증하고 내부 구조는 보지 않는다. */
class LoadCliTest {
    @TempDir
    Path dir;

    private static final class Outcome {
        int exit;
        String out;
        String err;
    }

    private Outcome run(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        Outcome outcome = new Outcome();
        outcome.exit = LoadCli.execute(
                args,
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
        outcome.out = out.toString(StandardCharsets.UTF_8);
        outcome.err = err.toString(StandardCharsets.UTF_8);
        return outcome;
    }

    private JsonObject readResult(Path file) throws Exception {
        return JsonParser.parseString(Files.readString(file)).getAsJsonObject();
    }

    private static JsonObject firstRun(JsonObject json) {
        return json.getAsJsonArray("runs").get(0).getAsJsonObject();
    }

    @ParameterizedTest
    @ValueSource(strings = {"nio", "netty"})
    void smokeRunEndsWithoutErrorsAndRecordsTheServerTransport(String transport) throws Exception {
        Path result = dir.resolve("result.json");

        Outcome outcome = run(
                "run",
                "--transport",
                transport,
                "--connections",
                "1",
                "--body-bytes",
                "64",
                "--warmup",
                "500ms",
                "--measure",
                "1500ms",
                "--output",
                result.toString());

        assertThat(outcome.exit).as(outcome.err).isZero();
        JsonObject json = readResult(result);
        assertThat(json.get("schemaVersion").getAsInt()).isEqualTo(1);
        assertThat(json.get("status").getAsString()).isEqualTo("OK");
        JsonObject metadata = json.getAsJsonObject("metadata");
        assertThat(metadata.get("jdk").getAsString()).isNotBlank();
        assertThat(metadata.get("os").getAsString()).isNotBlank();
        assertThat(metadata.get("cores").getAsInt()).isPositive();
        assertThat(metadata.has("gitSha")).isTrue();
        JsonObject config = metadata.getAsJsonObject("config");
        assertThat(config.get("transport").getAsString()).isEqualTo(transport);
        // 부하 생성기 코드 경로는 서버 전송과 무관하게 고정이다.
        assertThat(config.get("loadgenTransport").getAsString()).isEqualTo("nio");
        assertThat(config.get("connections").getAsInt()).isEqualTo(1);
        assertThat(config.get("bodyBytes").getAsInt()).isEqualTo(64);
        assertThat(config.get("loadModel").getAsString()).isEqualTo("closed-loop");
        assertThat(config.get("tls").getAsBoolean()).isFalse();

        JsonObject measurement = firstRun(json).getAsJsonObject("measurement");
        long requests = measurement.get("requests").getAsLong();
        assertThat(requests).isPositive();
        assertThat(measurement.get("responses").getAsLong()).isEqualTo(requests);
        assertThat(measurement.get("errors").getAsLong()).isZero();
        assertThat(measurement.get("unknown").getAsLong()).isZero();
        assertThat(measurement.get("throughputPerSecond").getAsDouble()).isPositive();
    }

    @Test
    void processesAreGoneAfterTheRun() throws Exception {
        Path result = dir.resolve("result.json");

        Outcome outcome = run("run", "--warmup", "200ms", "--measure", "500ms", "--output", result.toString());

        assertThat(outcome.exit).as(outcome.err).isZero();
        JsonObject processes = firstRun(readResult(result)).getAsJsonObject("processes");
        for (String role : new String[] {"server", "loadgen"}) {
            long pid = processes.get(role).getAsLong();
            assertThat(ProcessHandle.of(pid).filter(ProcessHandle::isAlive))
                    .as(role + " pid " + pid)
                    .isEmpty();
        }
    }

    @Test
    void invalidConfigurationFailsBeforeAnyProcessStarts() {
        Path result = dir.resolve("result.json");

        Outcome outcome = run("run", "--connections", "0", "--output", result.toString());

        assertThat(outcome.exit).isEqualTo(2);
        assertThat(outcome.err).contains("--connections");
        assertThat(result).doesNotExist();
    }

    @Test
    void unknownTransportFailsWithAClearMessage() {
        Outcome outcome = run("run", "--transport", "carrier-pigeon");

        assertThat(outcome.exit).isEqualTo(2);
        assertThat(outcome.err).contains("--transport").contains("carrier-pigeon");
    }

    @Test
    void unknownOptionFailsWithAClearMessage() {
        Outcome outcome = run("run", "--no-such-option", "1");

        assertThat(outcome.exit).isEqualTo(2);
        assertThat(outcome.err).contains("--no-such-option");
    }

    @Test
    void abnormalServerExitFailsTheWholeRunAndLeavesNoPartialResult() {
        Path result = dir.resolve("result.json");

        Outcome outcome = run(
                "run",
                "--warmup",
                "300ms",
                "--measure",
                "5s",
                "--output",
                result.toString(),
                "--chaos-kill-server-after",
                "700ms");

        assertThat(outcome.exit).isEqualTo(1);
        assertThat(outcome.err).contains("서버");
        assertThat(result).doesNotExist();
    }

    private JsonObject openRun(String... extra) throws Exception {
        Path result = dir.resolve("open.json");
        java.util.List<String> args = new java.util.ArrayList<>(java.util.List.of(
                "run",
                "--load-model",
                "open",
                "--rate",
                "50",
                "--warmup",
                "300ms",
                "--measure",
                "2s",
                "--repetitions",
                "1",
                "--output",
                result.toString()));
        args.addAll(java.util.Arrays.asList(extra));
        Outcome outcome = run(args.toArray(new String[0]));
        assertThat(outcome.exit).as(outcome.err).isZero();
        return readResult(result);
    }

    private static double latencyMicros(JsonObject run, String percentile) {
        return run.getAsJsonObject("measurement")
                .getAsJsonObject("latencyMicros")
                .get(percentile)
                .getAsDouble();
    }

    @Test
    void openLoopReportsPercentilesAndKeepsTheScheduledRate() throws Exception {
        JsonObject json = openRun();

        assertThat(json.getAsJsonObject("metadata")
                        .getAsJsonObject("config")
                        .get("loadModel")
                        .getAsString())
                .isEqualTo("open-loop");
        JsonObject run = firstRun(json);
        assertThat(latencyMicros(run, "p50")).isPositive();
        assertThat(latencyMicros(run, "p99")).isGreaterThanOrEqualTo(latencyMicros(run, "p50"));
        assertThat(latencyMicros(run, "p999")).isGreaterThanOrEqualTo(latencyMicros(run, "p99"));
        JsonObject measurement = run.getAsJsonObject("measurement");
        assertThat(measurement.get("errors").getAsLong()).isZero();
        assertThat(measurement.get("responses").getAsLong())
                .isEqualTo(measurement.get("requests").getAsLong());
        // 50 req/s를 2초 재면 약 100건이다.
        assertThat(measurement.get("requests").getAsLong()).isBetween(80L, 120L);
    }

    @Test
    void openLoopP99ReflectsInjectedServerDelay() throws Exception {
        JsonObject delayed = openRun("--server-delay", "100ms");

        // 서버에 넣은 100ms 지연이 예정 시각 기준 지연의 p99에 그대로 드러나야 한다(coordinated omission 방지).
        assertThat(latencyMicros(firstRun(delayed), "p99")).isGreaterThanOrEqualTo(95_000);
        assertThat(firstRun(delayed)
                        .getAsJsonObject("measurement")
                        .get("errors")
                        .getAsLong())
                .isZero();
    }

    @Test
    void repetitionsAreSummarizedAsMedianAndRange() throws Exception {
        Path result = dir.resolve("reps.json");

        Outcome outcome = run(
                "run",
                "--load-model",
                "open",
                "--rate",
                "50",
                "--warmup",
                "100ms",
                "--measure",
                "500ms",
                "--repetitions",
                "3",
                "--output",
                result.toString());

        assertThat(outcome.exit).as(outcome.err).isZero();
        JsonObject json = readResult(result);
        assertThat(json.getAsJsonArray("runs")).hasSize(3);
        JsonObject p99 =
                json.getAsJsonObject("summary").getAsJsonObject("latencyMicros").getAsJsonObject("p99");
        assertThat(p99.get("min").getAsDouble())
                .isLessThanOrEqualTo(p99.get("median").getAsDouble());
        assertThat(p99.get("median").getAsDouble())
                .isLessThanOrEqualTo(p99.get("max").getAsDouble());
        assertThat(json.getAsJsonObject("summary")
                        .getAsJsonObject("throughputPerSecond")
                        .has("median"))
                .isTrue();
    }

    @Test
    void rateAsPercentOfSaturationIsResolvedFromAClosedLoopRun() throws Exception {
        JsonObject json = openRun("--rate", "50%");

        JsonObject config = json.getAsJsonObject("metadata").getAsJsonObject("config");
        double saturation = config.get("saturationThroughputPerSecond").getAsDouble();
        assertThat(saturation).isPositive();
        assertThat(config.get("rateTargetPerSecond").getAsDouble())
                .isCloseTo(saturation * 0.5, org.assertj.core.data.Offset.offset(1.0));
    }

    @Test
    void generatorThatCannotKeepTheScheduleIsFlagged() throws Exception {
        // 도달할 수 없는 도착률을 주면 부하 생성기가 병목이 되고 결과에 표시된다.
        JsonObject json = openRun("--rate", "50000000");

        assertThat(firstRun(json)
                        .getAsJsonObject("measurement")
                        .get("loadgenBottleneck")
                        .getAsBoolean())
                .isTrue();
    }

    @Test
    void openLoopWithoutRateIsAConfigurationError() {
        Outcome outcome = run("run", "--load-model", "open");

        assertThat(outcome.exit).isEqualTo(2);
        assertThat(outcome.err).contains("--rate");
    }
}
