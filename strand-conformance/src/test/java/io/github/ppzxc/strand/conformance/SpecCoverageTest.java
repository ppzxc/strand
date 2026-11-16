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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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

    /** 규범 문장이 있는 절 번호와 그 줄 수. */
    private static Map<String, Integer> normativeSections() throws IOException {
        return RequirementIndex.parse(new String(Files.readAllBytes(SPEC), StandardCharsets.UTF_8))
                .keywordSections();
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

    // ---- 벡터의 requirements (선택 필드) ----

    private static RequirementIndex index() throws IOException {
        return RequirementIndex.parse(new String(Files.readAllBytes(SPEC), StandardCharsets.UTF_8));
    }

    /** 벡터 키에서 그 벡터가 적은 요구사항 식별자로. 적지 않은 벡터는 빈 목록이다. */
    private static Map<String, List<String>> vectorRequirements() {
        Map<String, List<String>> result = new TreeMap<>();
        for (Map.Entry<String, JsonObject> vector : RawVectors.byKey().entrySet()) {
            List<String> ids = new ArrayList<>();
            if (vector.getValue().has("requirements")) {
                for (JsonElement id : vector.getValue().getAsJsonArray("requirements")) {
                    ids.add(id.getAsString());
                }
            }
            result.put(vector.getKey(), ids);
        }
        return result;
    }

    private static List<String> deletedRequirementIds() throws IOException {
        List<String> result = new ArrayList<>();
        try (Reader reader = new InputStreamReader(
                        SpecCoverageTest.class.getResourceAsStream("/deleted-requirement-ids.txt"),
                        StandardCharsets.UTF_8);
                BufferedReader lines = new BufferedReader(reader)) {
            lines.lines().filter(l -> !l.isEmpty() && !l.startsWith("#")).forEach(result::add);
        }
        return result;
    }

    /** 규격 색인에 없는 식별자를 가리키는 벡터. 어긋난 곳을 이름으로 짚는다. */
    private static List<String> unknownReferences(RequirementIndex index, Map<String, List<String>> byVector) {
        List<String> problems = new ArrayList<>();
        for (Map.Entry<String, List<String>> vector : byVector.entrySet()) {
            for (String id : vector.getValue()) {
                if (index.requirement(id) == null) {
                    problems.add(vector.getKey() + "의 requirements가 규격에 없는 식별자를 가리킨다: " + id);
                }
            }
        }
        return problems;
    }

    /** SHOULD·SHOULD NOT 요구사항을 가리키는 벡터. 따르지 않아도 되는 행동은 단언하지 않는다. */
    private static List<String> shouldReferences(RequirementIndex index, Map<String, List<String>> byVector) {
        List<String> problems = new ArrayList<>();
        for (Map.Entry<String, List<String>> vector : byVector.entrySet()) {
            for (String id : vector.getValue()) {
                RequirementIndex.Requirement requirement = index.requirement(id);
                if (requirement != null && !requirement.keyword().mandatory()) {
                    problems.add(vector.getKey() + "의 requirements가 " + requirement.keyword() + " 요구사항을 가리킨다: " + id);
                }
            }
        }
        return problems;
    }

    /** 삭제 목록의 이름이 규격 앵커나 벡터 requirements에 다시 나온 자리. */
    private static List<String> revivedDeletedIds(
            RequirementIndex index, Map<String, List<String>> byVector, List<String> deleted) {
        List<String> problems = new ArrayList<>();
        for (String id : deleted) {
            if (index.requirement(id) != null) {
                problems.add("지운 식별자가 규격 앵커로 다시 나온다: " + id);
            }
            for (Map.Entry<String, List<String>> vector : byVector.entrySet()) {
                if (vector.getValue().contains(id)) {
                    problems.add("지운 식별자를 " + vector.getKey() + "의 requirements가 가리킨다: " + id);
                }
            }
        }
        return problems;
    }

    @Test
    void vectorsPointOnlyToExistingMandatoryRequirements() throws IOException {
        assertThat(unknownReferences(index(), vectorRequirements())).isEmpty();
        assertThat(shouldReferences(index(), vectorRequirements())).isEmpty();
    }

    @Test
    void deletedRequirementIdsDoNotReturn() throws IOException {
        assertThat(revivedDeletedIds(index(), vectorRequirements(), deletedRequirementIds()))
                .isEmpty();
    }

    @Test
    void unknownIdentifierIsNamedInTheFailure() {
        RequirementIndex index = RequirementIndex.parse("## 2. 절\n보낸다(MUST)<a id=\"req-known\"></a>\n");
        Map<String, List<String>> vectors = new TreeMap<>();
        vectors.put("f.json/ok", Arrays.asList("req-known"));
        vectors.put("f.json/typo", Arrays.asList("req-knwon"));

        assertThat(unknownReferences(index, vectors))
                .hasSize(1)
                .allMatch(p -> p.contains("f.json/typo") && p.contains("req-knwon"));
    }

    @Test
    void shouldRequirementsCannotBeReferenced() {
        RequirementIndex index = RequirementIndex.parse("## 2. 절\n"
                + "하나(MUST)<a id=\"req-m\"></a>\n"
                + "둘(MUST NOT)<a id=\"req-mn\"></a>\n"
                + "셋(SHOULD)<a id=\"req-s\"></a>\n"
                + "넷(SHOULD NOT)<a id=\"req-sn\"></a>\n");
        Map<String, List<String>> vectors = new TreeMap<>();
        vectors.put("f.json/a", Arrays.asList("req-m", "req-mn", "req-s"));
        vectors.put("f.json/b", Arrays.asList("req-sn"));

        assertThat(shouldReferences(index, vectors))
                .hasSize(2)
                .anyMatch(p -> p.contains("f.json/a") && p.contains("req-s"))
                .anyMatch(p -> p.contains("f.json/b") && p.contains("req-sn"));
    }

    @Test
    void deletedNameInASpecAnchorOrVectorFails() {
        RequirementIndex index = RequirementIndex.parse("## 2. 절\n보낸다(MUST)<a id=\"req-old\"></a>\n");
        Map<String, List<String>> vectors = new TreeMap<>();
        vectors.put("f.json/a", Arrays.asList("req-gone"));

        assertThat(revivedDeletedIds(index, vectors, Arrays.asList("req-old", "req-gone", "req-never")))
                .hasSize(2)
                .anyMatch(p -> p.contains("req-old"))
                .anyMatch(p -> p.contains("req-gone") && p.contains("f.json/a"));
    }
}
