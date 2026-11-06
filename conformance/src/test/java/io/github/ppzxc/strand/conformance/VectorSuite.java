package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
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
            // 테스트 이름은 `파일명/id`(벡터 형식 문서 3절).
            String name = fileKey(file);
            for (ConformanceVector vector : VectorLoader.load(file)) {
                tests.add(DynamicTest.dynamicTest(name + "/" + vector.id(), () -> run(name, vector)));
            }
        }
        return tests.stream();
    }

    /** 테스트 이름과 벡터 키에 쓰는 이름: 파일명에서 `.json`을 뺀 것(벡터 형식 문서 3절). */
    static String fileKey(Path file) {
        String fileName = file.getFileName().toString();
        return fileName.substring(0, fileName.length() - ".json".length());
    }

    /** 디렉터리의 벡터 파일(`*.json`)을 이름순으로. */
    static List<Path> jsonFiles(Path directory) {
        try (Stream<Path> children = Files.list(directory)) {
            List<Path> files = children.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .collect(Collectors.toList());
            Collections.sort(files);
            return files;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 러너가 실행하는 벡터 파일. 규격 구현이 들어올 때마다 해당 파일을 여기에 더한다. 나머지는 skipped로 보인다. */
    private static final Set<String> IMPLEMENTED_FILES = new HashSet<>(Arrays.asList("sample", "handshake"));

    // 로드와 구조 검증은 이미 끝났다(dynamicTests가 로더를 부르는 시점에 실패로 드러난다).
    // 구현이 아직 없는 파일의 벡터는 통과로 위장하지 않고 skipped로 보인다.
    private static void run(String fileKey, ConformanceVector vector) {
        if (!IMPLEMENTED_FILES.contains(fileKey)) {
            Assumptions.abort("core 미구현: " + fileKey + " 벡터 실행을 건너뛴다 (" + vector.id() + ")");
        }
        VectorRunner.run(vector);
    }
}
