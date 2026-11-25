package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestReporter;
import org.junit.jupiter.api.io.TempDir;

class VectorConformanceTest {

    private static final String VECTOR_TEMPLATE =
            "{\"id\":\"%s\",\"description\":\"d\",\"requirements\":[\"req-ping-ack-reply\"],"
                    + "\"setup\":{\"role\":\"server\",\"established\":true},"
                    + "\"steps\":[{\"kind\":\"recv\",\"hex\":\"00\",\"send\":[],\"events\":[]}]}";

    private static void writeFile(Path dir, String name, String... ids) throws IOException {
        String vectors =
                Stream.of(ids).map(id -> String.format(VECTOR_TEMPLATE, id)).collect(Collectors.joining(","));
        Files.write(
                dir.resolve(name), ("{\"format\":2,\"vectors\":[" + vectors + "]}").getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> names(Path dir) {
        return VectorSuite.dynamicTests(dir).map(DynamicTest::getDisplayName).collect(Collectors.toList());
    }

    @Test
    void realVectorFilesPassTheLoader() {
        List<Path> files = VectorSuite.jsonFiles(VectorSuite.defaultDirectory());
        assertThat(files).isNotEmpty();
        for (Path file : files) {
            assertThat(VectorLoader.load(file))
                    .as(file.getFileName().toString())
                    .isNotEmpty();
        }
    }

    @TestFactory
    Stream<DynamicTest> vectors() {
        if (!"integrated".equals(executionPath())) return Stream.empty();
        return VectorSuite.dynamicTests(VectorSuite.defaultDirectory());
    }

    @Test
    void integratedExecutionPathReportHasNoFailuresOrUnsupportedVectors(TestReporter reporter) {
        Assumptions.assumeTrue("integrated".equals(executionPath()));
        List<VectorOutcome> coreOutcomes = new ArrayList<>();
        List<VectorOutcome> nioOutcomes = new ArrayList<>();
        List<VectorOutcome> nettyOutcomes = new ArrayList<>();
        for (Path file : VectorSuite.jsonFiles(VectorSuite.defaultDirectory())) {
            String fileKey = VectorSuite.fileKey(file);
            for (ConformanceVector vector : VectorLoader.load(file)) {
                String vectorId = fileKey + "/" + vector.id();
                coreOutcomes.add(ExecutionPaths.CORE.evaluate(vector, vectorId));
                nioOutcomes.add(ExecutionPaths.NIO.evaluate(vector, vectorId));
                nettyOutcomes.add(ExecutionPaths.NETTY.evaluate(vector, vectorId));
            }
        }
        Map<String, List<VectorOutcome>> outcomesByPath = new LinkedHashMap<>();
        outcomesByPath.put(ConformanceReport.CORE_PATH, coreOutcomes);
        outcomesByPath.put(ConformanceReport.NIO_PATH, nioOutcomes);
        outcomesByPath.put(ConformanceReport.NETTY_PATH, nettyOutcomes);
        ConformanceReport coreReport = ConformanceReport.forPath(ConformanceReport.CORE_PATH, coreOutcomes);
        ConformanceReport nioReport = ConformanceReport.forPath(ConformanceReport.NIO_PATH, nioOutcomes);
        ConformanceReport nettyReport = ConformanceReport.forPath(ConformanceReport.NETTY_PATH, nettyOutcomes);
        ConformanceReport integrated = ConformanceReport.integrated(outcomesByPath);

        reporter.publishEntry(ConformanceReport.CORE_PATH, coreReport.summary());
        reporter.publishEntry(ConformanceReport.NIO_PATH, nioReport.summary());
        reporter.publishEntry(ConformanceReport.NETTY_PATH, nettyReport.summary());
        reporter.publishEntry(
                ConformanceReport.INTEGRATED_PATH,
                (integrated.passes() ? "전체 적합성 통과: " : "전체 적합성 실패: ") + integrated.summary());

        assertThat(coreReport.count(VectorOutcome.Status.FAIL))
                .as("core 경로 결과: %s", coreReport.summary())
                .isZero();
        assertThat(nioReport.count(VectorOutcome.Status.FAIL))
                .as("nio 경로 결과: %s", nioReport.summary())
                .isZero();
        assertThat(nettyReport.count(VectorOutcome.Status.FAIL))
                .as("netty 경로 결과: %s", nettyReport.summary())
                .isZero();
        int vectorCount = VectorSuite.jsonFiles(VectorSuite.defaultDirectory()).stream()
                .mapToInt(file -> VectorLoader.load(file).size())
                .sum();
        assertThat(coreOutcomes).hasSize(vectorCount);
        assertThat(nioOutcomes).hasSize(vectorCount);
        assertThat(nettyOutcomes).hasSize(vectorCount);
        assertThat(integrated.passes())
                .as("통합 적합성 결과: %s", integrated.summary())
                .isTrue();
    }

    @Test
    void nettyPathPassesEveryWebSocketVectorAndReportsTheRestUnsupported() {
        int webSocketVectors = 0;
        for (Path file : VectorSuite.jsonFiles(VectorSuite.defaultDirectory())) {
            for (ConformanceVector vector : VectorLoader.load(file)) {
                VectorOutcome outcome =
                        ExecutionPaths.NETTY.evaluate(vector, VectorSuite.fileKey(file) + "/" + vector.id());
                boolean webSocket = vector.setup().has("transport")
                        && VectorSchema.WEBSOCKET.equals(
                                vector.setup().get("transport").getAsString());
                if (webSocket) {
                    webSocketVectors++;
                    assertThat(outcome.status())
                            .as("%s: %s", outcome.vectorId(), outcome.details())
                            .isEqualTo(VectorOutcome.Status.PASS);
                } else {
                    assertThat(outcome.status()).isEqualTo(VectorOutcome.Status.UNSUPPORTED);
                }
            }
        }
        assertThat(webSocketVectors).isPositive();
    }

    @Test
    void webSocketCloseVectorsAreUnsupportedOnCoreAndPassOnNetty() {
        int closeVectors = 0;
        for (Path file : VectorSuite.jsonFiles(VectorSuite.defaultDirectory())) {
            for (ConformanceVector vector : VectorLoader.load(file)) {
                if (!vector.steps().toString().contains("WEBSOCKET_CLOSE")) {
                    continue;
                }
                closeVectors++;
                String id = VectorSuite.fileKey(file) + "/" + vector.id();
                assertThat(ExecutionPaths.CORE.evaluate(vector, id).status())
                        .isEqualTo(VectorOutcome.Status.UNSUPPORTED);
                assertThat(ExecutionPaths.NETTY.evaluate(vector, id).status()).isEqualTo(VectorOutcome.Status.PASS);
            }
        }
        assertThat(closeVectors).isPositive();
    }

    @Test
    void selectedExecutionPathReportHasNoFailuresOrUnsupportedVectors(TestReporter reporter) {
        String path = executionPath();
        Assumptions.assumeTrue(!ConformanceReport.INTEGRATED_PATH.equals(path));
        ExecutionPath executionPath = ExecutionPaths.named(path);
        List<VectorOutcome> outcomes = new ArrayList<>();
        for (Path file : VectorSuite.jsonFiles(VectorSuite.defaultDirectory())) {
            String fileKey = VectorSuite.fileKey(file);
            for (ConformanceVector vector : VectorLoader.load(file)) {
                String vectorId = fileKey + "/" + vector.id();
                outcomes.add(executionPath.evaluate(vector, vectorId));
            }
        }
        ConformanceReport report = ConformanceReport.forPath(path, outcomes);
        reporter.publishEntry(path, report.summary());

        assertThat(report.passes())
                .as("%s 실행 경로 적합성 결과: %s", path, report.summary())
                .isTrue();
    }

    private static String executionPath() {
        return System.getProperty("strand.conformance.execution.path", ConformanceReport.INTEGRATED_PATH);
    }

    @Test
    void defaultDirectoryIsAnAbsolutePathPassedByTheBuild() {
        assertThat(VectorSuite.defaultDirectory()).isAbsolute().isDirectory().hasFileName("vectors");
    }

    @Test
    void testsAreNamedByFileNameWithoutExtensionAndId(@TempDir Path dir) throws IOException {
        writeFile(dir, "streams.json", "b-one", "a-two");
        writeFile(dir, "framing.json", "x");

        assertThat(names(dir)).containsExactly("framing/x", "streams/b-one", "streams/a-two");
    }

    @Test
    void sameIdInDifferentFilesIsAllowed(@TempDir Path dir) throws IOException {
        writeFile(dir, "one.json", "a");
        writeFile(dir, "two.json", "a");

        assertThat(names(dir)).containsExactly("one/a", "two/a");
    }

    @Test
    void addingAVectorFileDoesNotBreakExistingTests(@TempDir Path dir) throws IOException {
        writeFile(dir, "one.json", "a");
        List<String> before = names(dir);

        writeFile(dir, "two.json", "b");

        assertThat(names(dir)).containsAll(before).hasSize(before.size() + 1);
    }

    @Test
    void vectorsAreExecutedAndMismatchesFail(@TempDir Path dir) throws IOException {

        String vector = "{\"id\":\"x\",\"description\":\"d\",\"requirements\":[\"req-ping-ack-reply\"],"
                + "\"setup\":{\"role\":\"server\",\"established\":true},"
                + "\"steps\":[{\"kind\":\"recv\",\"hex\":\"00\",\"send\":[],"
                + "\"events\":[{\"event\":\"closed\"}]}]}";
        Files.write(
                dir.resolve("sample.json"),
                ("{\"format\":2,\"vectors\":[" + vector + "]}").getBytes(StandardCharsets.UTF_8));
        DynamicTest test = VectorSuite.dynamicTests(dir).findFirst().get();

        assertThatThrownBy(() -> test.getExecutable().execute()).isInstanceOf(AssertionError.class);
    }

    @Test
    void invalidVectorFileFailsInsteadOfBeingSkipped(@TempDir Path dir) throws IOException {
        Files.write(dir.resolve("bad.json"), "[]".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> VectorSuite.dynamicTests(dir)).isInstanceOf(InvalidVectorException.class);
    }
}
