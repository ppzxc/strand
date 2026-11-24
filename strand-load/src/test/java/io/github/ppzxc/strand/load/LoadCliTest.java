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

    @Test
    void smokeRunWithNioEndsWithoutErrorsAndWritesTheResultJson() throws Exception {
        Path result = dir.resolve("result.json");

        Outcome outcome = run(
                "run",
                "--transport",
                "nio",
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
        assertThat(config.get("transport").getAsString()).isEqualTo("nio");
        assertThat(config.get("connections").getAsInt()).isEqualTo(1);
        assertThat(config.get("bodyBytes").getAsInt()).isEqualTo(64);
        assertThat(config.get("loadModel").getAsString()).isEqualTo("closed-loop");
        assertThat(config.get("tls").getAsBoolean()).isFalse();

        JsonObject measurement = json.getAsJsonObject("measurement");
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
        JsonObject processes = readResult(result).getAsJsonObject("metadata").getAsJsonObject("processes");
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
}
