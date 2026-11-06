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

    private static final String VECTOR_TEMPLATE = "{\"id\":\"%s\",\"description\":\"d\",\"spec\":[\"3.4\"],"
            + "\"setup\":{\"role\":\"server\",\"established\":true},"
            + "\"steps\":[{\"kind\":\"recv\",\"hex\":\"00\",\"send\":[],\"events\":[]}]}";

    private static void writeFile(Path dir, String name, String... ids) throws IOException {
        String vectors =
                Stream.of(ids).map(id -> String.format(VECTOR_TEMPLATE, id)).collect(Collectors.joining(","));
        Files.write(
                dir.resolve(name), ("{\"format\":1,\"vectors\":[" + vectors + "]}").getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> names(Path dir) {
        return VectorSuite.dynamicTests(dir).map(DynamicTest::getDisplayName).collect(Collectors.toList());
    }

    /** 실제 벡터 디렉터리는 벡터 목록을 단언하지 않는다. 벡터 파일이 추가돼도 깨지지 않게, 읽히는지만 본다. */
    @TestFactory
    Stream<DynamicTest> vectors() {
        return VectorSuite.dynamicTests(VectorSuite.defaultDirectory());
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
        String vector = "{\"id\":\"x\",\"description\":\"d\",\"spec\":[\"3.4\"],"
                + "\"setup\":{\"role\":\"server\",\"established\":true},"
                + "\"steps\":[{\"kind\":\"recv\",\"hex\":\"00\",\"send\":[],"
                + "\"events\":[{\"event\":\"closed\"}]}]}";
        Files.write(
                dir.resolve("sample.json"),
                ("{\"format\":1,\"vectors\":[" + vector + "]}").getBytes(StandardCharsets.UTF_8));
        DynamicTest test = VectorSuite.dynamicTests(dir).findFirst().get();

        assertThatThrownBy(() -> test.getExecutable().execute()).isInstanceOf(AssertionError.class);
    }

    @Test
    void invalidVectorFileFailsInsteadOfBeingSkipped(@TempDir Path dir) throws IOException {
        Files.write(dir.resolve("bad.json"), "[]".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> VectorSuite.dynamicTests(dir)).isInstanceOf(InvalidVectorException.class);
    }
}
