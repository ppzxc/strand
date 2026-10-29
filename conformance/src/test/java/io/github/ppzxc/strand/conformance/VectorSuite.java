package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

/** 벡터 디렉터리를 읽어 벡터마다 동적 테스트를 만든다. */
final class VectorSuite {

    private VectorSuite() {}

    /** Gradle test 작업의 작업 디렉터리는 모듈 디렉터리이므로 {@code conformance/vectors/}를 가리킨다. */
    static Path defaultDirectory() {
        return Paths.get("vectors");
    }

    static Stream<DynamicTest> dynamicTests(Path directory) {
        List<Path> files = jsonFiles(directory);
        assertThat(files).as("벡터 파일이 하나도 없다: %s", directory.toAbsolutePath()).isNotEmpty();
        List<DynamicTest> tests = new ArrayList<>();
        for (Path file : files) {
            String fileName = file.getFileName().toString();
            for (Vector vector : VectorLoader.load(file)) {
                tests.add(DynamicTest.dynamicTest(fileName + "/" + vector.id(), () -> run(vector)));
            }
        }
        return tests.stream();
    }

    private static List<Path> jsonFiles(Path directory) {
        try (Stream<Path> children = Files.list(directory)) {
            List<Path> files = children.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .collect(Collectors.toList());
            Collections.sort(files);
            return files;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // core에 규격 구현이 생기기 전까지는 로더가 받아들였다는 사실 외에 검증할 대상이 없다.
    // 규격 구현이 들어오면 여기서 vector.bytes()를 core에 넘기고 result와 비교한다.
    private static void run(Vector vector) {
        assertThat(vector.result()).isNotNull();
    }
}
