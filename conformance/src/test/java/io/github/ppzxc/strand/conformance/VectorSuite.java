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

    static final String VECTORS_DIR_PROPERTY = "strand.conformance.vectors.dir";

    /** 빌드가 {@value #VECTORS_DIR_PROPERTY} 시스템 프로퍼티로 넘긴 벡터 디렉터리. 다른 디렉터리의 벡터를 돌릴 때도 같은 프로퍼티를 쓴다. */
    static Path defaultDirectory() {
        String dir = System.getProperty(VECTORS_DIR_PROPERTY);
        if (dir == null) {
            throw new IllegalStateException("시스템 프로퍼티 " + VECTORS_DIR_PROPERTY + "가 없다");
        }
        return Paths.get(dir);
    }

    static Stream<DynamicTest> dynamicTests(Path directory) {
        List<Path> files = jsonFiles(directory);
        assertThat(files).as("벡터 파일이 하나도 없다: %s", directory.toAbsolutePath()).isNotEmpty();
        List<DynamicTest> tests = new ArrayList<>();
        for (Path file : files) {
            String fileName = file.getFileName().toString();
            for (ConformanceVector vector : VectorLoader.load(file)) {
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

    // core에 규격 구현이 생기기 전까지는 로더가 받아들인 필드가 일관된지만 확인한다(내용 단언은 로더 테스트가 한다).
    // 규격 구현이 들어오면 여기서 vector.bytes()를 core에 넘기고 result와 비교한다.
    private static void run(ConformanceVector vector) {
        assertThat(vector.id()).isNotEmpty();
        assertThat(vector.description()).isNotNull();
        assertThat(vector.result()).isIn(ConformanceVector.Result.VALID, ConformanceVector.Result.INVALID);
        assertThat(vector.bytes()).isNotNull();
    }
}
