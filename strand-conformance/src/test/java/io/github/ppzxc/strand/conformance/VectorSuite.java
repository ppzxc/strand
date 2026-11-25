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

final class VectorSuite {

    private VectorSuite() {}

    static final String VECTORS_DIR_PROPERTY = "strand.conformance.vectors.dir";

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

            String name = fileKey(file);
            for (ConformanceVector vector : VectorLoader.load(file)) {
                String id = name + "/" + vector.id();
                tests.add(DynamicTest.dynamicTest(id, () -> {
                    VectorOutcome core = ExecutionPaths.CORE.evaluate(vector, id);
                    VectorOutcome nio = ExecutionPaths.NIO.evaluate(vector, id);
                    VectorOutcome netty = ExecutionPaths.NETTY.evaluate(vector, id);
                    VectorOutcome outcome = ConformanceReport.integratedOutcome(id, core, nio, netty);
                    assertThat(outcome.status())
                            .as("벡터 %s (%s): %s", id, outcome.status(), outcome.details())
                            .isEqualTo(VectorOutcome.Status.PASS);
                }));
            }
        }
        return tests.stream();
    }

    static String fileKey(Path file) {
        String fileName = file.getFileName().toString();
        return fileName.substring(0, fileName.length() - ".json".length());
    }

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
}
