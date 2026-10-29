package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/** {@code conformance/vectors/}의 모든 벡터 파일을 읽어 벡터마다 동적 테스트 하나를 만든다. 이름은 {@code 파일명/id}. */
class VectorConformanceTest {

    @TestFactory
    Stream<DynamicTest> vectors() {
        return VectorSuite.dynamicTests(VectorSuite.defaultDirectory());
    }

    @Test
    void sampleVectorsLoadAndAreNamedByFileAndId() {
        List<String> names = vectors().map(DynamicTest::getDisplayName).collect(Collectors.toList());

        assertThat(names)
                .containsExactly("sample.json/empty-valid", "sample.json/ping-valid", "sample.json/truncated-invalid");
    }
}
