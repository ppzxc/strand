package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/** 벡터 디렉터리의 모든 벡터 파일을 읽어 벡터마다 동적 테스트 하나를 만든다. 이름은 {@code 파일명/id}(파일명의 {@code .json}은 뺀다). */
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

    /** 실제 벡터 디렉터리의 모든 파일이 형식 2 로더를 통과하는지 본다. core를 실행하지 않는다. 벡터 목록은 단언하지 않는다. */
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

    /** 실제 벡터 디렉터리의 각 벡터를 지원하는 실행 경로에서 실행한다. 벡터 목록은 단언하지 않는다. */
    @TestFactory
    Stream<DynamicTest> vectors() {
        return VectorSuite.dynamicTests(VectorSuite.defaultDirectory());
    }

    /** 실제 경로 결과를 합쳐 보고한다. core 미지원 벡터는 지원하는 전송 경로에서 통과해야 한다. */
    @Test
    void integratedExecutionPathReportHasNoFailuresOrUnsupportedVectors() {
        List<VectorOutcome> coreOutcomes = new java.util.ArrayList<>();
        List<VectorOutcome> nioOutcomes = new java.util.ArrayList<>();
        for (Path file : VectorSuite.jsonFiles(VectorSuite.defaultDirectory())) {
            String fileKey = VectorSuite.fileKey(file);
            for (ConformanceVector vector : VectorLoader.load(file)) {
                String vectorId = fileKey + "/" + vector.id();
                coreOutcomes.add(VectorRunner.evaluate(vector, vectorId));
                if (NioVectorRunner.supports(vector)) {
                    nioOutcomes.add(NioVectorRunner.evaluate(vector, vectorId));
                }
            }
        }
        java.util.Map<String, List<VectorOutcome>> outcomesByPath = new java.util.LinkedHashMap<>();
        outcomesByPath.put("core", coreOutcomes);
        outcomesByPath.put("nio", nioOutcomes);
        ConformanceReport coreReport = ConformanceReport.forPath("core", coreOutcomes);
        ConformanceReport nioReport = ConformanceReport.forPath("nio", nioOutcomes);
        ConformanceReport integrated = ConformanceReport.integrated(outcomesByPath);

        assertThat(coreReport.count(VectorOutcome.Status.FAIL))
                .as("core 경로 실행 실패: %s", coreReport.summary())
                .isZero();
        assertThat(coreReport.count(VectorOutcome.Status.UNSUPPORTED))
                .as("core 미지원은 다른 경로 실행을 요구한다: %s", coreReport.summary())
                .isEqualTo(nioOutcomes.size());
        List<String> coreUnsupportedIds = coreOutcomes.stream()
                .filter(outcome -> outcome.status() == VectorOutcome.Status.UNSUPPORTED)
                .map(VectorOutcome::vectorId)
                .collect(Collectors.toList());
        List<String> nioPassedIds = nioOutcomes.stream()
                .filter(outcome -> outcome.status() == VectorOutcome.Status.PASS)
                .map(VectorOutcome::vectorId)
                .collect(Collectors.toList());
        assertThat(nioPassedIds).containsExactlyElementsOf(coreUnsupportedIds);
        assertThat(nioReport.passes())
                .as("NIO 경로 적합성 결과: %s", nioReport.summary())
                .isTrue();
        assertThat(integrated.passes())
                .as("통합 적합성 결과: %s", integrated.summary())
                .isTrue();
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
        // 아무것도 보내지 않을 입력에 closed를 기대하면 실패해야 한다.
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
