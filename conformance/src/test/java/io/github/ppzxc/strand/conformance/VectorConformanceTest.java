package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

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

/** {@code conformance/vectors/}의 모든 벡터 파일을 읽어 벡터마다 동적 테스트 하나를 만든다. 이름은 {@code 파일명/id}. */
class VectorConformanceTest {

    @TestFactory
    Stream<DynamicTest> vectors() {
        return VectorSuite.dynamicTests(VectorSuite.defaultDirectory());
    }

    @Test
    void defaultDirectoryIsAnAbsolutePathPassedByTheBuild() {
        assertThat(VectorSuite.defaultDirectory()).isAbsolute().isDirectory().hasFileName("vectors");
    }

    @Test
    void sampleVectorsDecodeToTheirExpectedFields() {
        List<ConformanceVector> vectors =
                VectorLoader.load(VectorSuite.defaultDirectory().resolve("sample.json"));

        assertThat(vectors)
                .extracting(ConformanceVector::id)
                .containsExactly("empty-valid", "ping-valid", "truncated-invalid");
        assertThat(vectors)
                .extracting(ConformanceVector::result)
                .containsExactly(
                        ConformanceVector.Result.VALID,
                        ConformanceVector.Result.VALID,
                        ConformanceVector.Result.INVALID);
        assertThat(vectors.get(0).bytes()).isEmpty();
        assertThat(vectors.get(1).bytes()).containsExactly((byte) 0x01, (byte) 0x00, (byte) 0xff);
        assertThat(vectors.get(2).bytes()).containsExactly((byte) 0x01);
    }

    @Test
    void sameIdInDifferentFilesIsAllowed(@TempDir Path dir) throws IOException {
        String json = "[{\"id\":\"a\",\"description\":\"d\",\"result\":\"valid\",\"bytes\":\"00\"}]";
        Files.write(dir.resolve("one.json"), json.getBytes(StandardCharsets.UTF_8));
        Files.write(dir.resolve("two.json"), json.getBytes(StandardCharsets.UTF_8));

        List<String> names =
                VectorSuite.dynamicTests(dir).map(DynamicTest::getDisplayName).collect(Collectors.toList());

        assertThat(names).containsExactly("one.json/a", "two.json/a");
    }

    @Test
    void sampleVectorsLoadAndAreNamedByFileAndId() {
        List<String> names = vectors().map(DynamicTest::getDisplayName).collect(Collectors.toList());

        assertThat(names)
                .containsExactly("sample.json/empty-valid", "sample.json/ping-valid", "sample.json/truncated-invalid");
    }
}
