package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * 규격과 벡터의 대응을 절 단위로 강제한다. 규범 문장(대문자 MUST, SHOULD)이 있는 절은 어떤 벡터의 {@code spec}이 가리키거나, {@code
 * uncovered-spec-sections.txt}에 사유와 함께 올라 있어야 한다. 문장 단위 대응은 보지 않는다. 절 안의 문장 하나가 빠져도 같은 절을 가리키는
 * 다른 벡터가 있으면 통과한다.
 */
class SpecCoverageTest {

    private static final Path SPEC = VectorSuite.defaultDirectory()
            .resolve("../../docs/specification/STRAND_SPECIFICATION.md")
            .normalize();

    private static final Pattern HEADING = Pattern.compile("^#{2,4} (\\d+(?:\\.\\d+)*)\\.?\\s");
    /** {@code \b}는 JDK 19부터 한글을 단어 문자로 보지 않아 JDK마다 결과가 달랐다. 영문자 기준 lookaround로 고정한다. */
    private static final Pattern NORMATIVE = Pattern.compile("(?<![A-Za-z])(MUST|SHOULD)(?![A-Za-z])");
    /** 용어 규칙을 설명하는 줄이라 규범 문장이 아니다. */
    private static final String KEYWORD_DEFINITION = "- MUST, MUST NOT";

    /** 규범 문장이 있는 절 번호와 그 줄 수. */
    private static Map<String, Integer> normativeSections() throws IOException {
        Map<String, Integer> result = new TreeMap<>();
        String current = null;
        for (String line : Files.readAllLines(SPEC, StandardCharsets.UTF_8)) {
            Matcher heading = HEADING.matcher(line);
            if (heading.find()) {
                current = heading.group(1);
            } else if (current != null
                    && !line.startsWith(KEYWORD_DEFINITION)
                    && NORMATIVE.matcher(line).find()) {
                result.merge(current, 1, Integer::sum);
            }
        }
        return result;
    }

    private static Set<String> referencedSections() {
        Set<String> result = new TreeSet<>();
        for (JsonObject vector : RawVectors.byKey().values()) {
            for (JsonElement ref : vector.getAsJsonArray("spec")) {
                result.add(ref.getAsString());
            }
        }
        return result;
    }

    /** 절 번호에서 사유로. */
    private static Map<String, String> exclusions() throws IOException {
        Map<String, String> result = new TreeMap<>();
        try (Reader reader = new InputStreamReader(
                        SpecCoverageTest.class.getResourceAsStream("/uncovered-spec-sections.txt"),
                        StandardCharsets.UTF_8);
                BufferedReader lines = new BufferedReader(reader)) {
            List<String> all = new ArrayList<>();
            lines.lines().filter(l -> !l.isEmpty() && !l.startsWith("#")).forEach(all::add);
            for (String line : all) {
                String[] parts = line.split("\\s+", 2);
                assertThat(parts).as("`절번호 사유` 형식: %s", line).hasSize(2);
                assertThat(result.put(parts[0], parts[1]))
                        .as("절이 두 번 올랐다: %s", parts[0])
                        .isNull();
            }
        }
        return result;
    }

    @Test
    void everyNormativeSectionIsCoveredByAVectorOrExcluded() throws IOException {
        Set<String> uncovered = new TreeSet<>(normativeSections().keySet());
        uncovered.removeAll(referencedSections());
        uncovered.removeAll(exclusions().keySet());
        assertThat(uncovered)
                .as("벡터가 가리키지 않는 규범 절. 벡터를 추가하거나 uncovered-spec-sections.txt에 사유와 함께 올린다")
                .isEmpty();
    }

    @Test
    void exclusionsAreNormativeSectionsNoVectorCovers() throws IOException {
        Set<String> normative = normativeSections().keySet();
        Set<String> referenced = referencedSections();
        for (String section : exclusions().keySet()) {
            assertThat(normative).as("%s절에는 규범 문장이 없다. 줄을 지운다", section).contains(section);
            assertThat(referenced).as("%s절을 벡터가 가리킨다. 줄을 지운다", section).doesNotContain(section);
        }
    }

    @Test
    void sectionScanFindsKnownNormativeSections() throws IOException {
        assertThat(normativeSections()).containsKeys("2.2.5", "3.3", "5.1");
        assertThat(normativeSections()).doesNotContainKey("1");
    }
}
