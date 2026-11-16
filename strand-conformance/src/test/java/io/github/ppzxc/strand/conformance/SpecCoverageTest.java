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

    // ---- 규격의 앵커 ----

    @Test
    void specHasNoAnchorProblems() throws IOException {
        assertThat(index().problems())
                .as("앵커가 없는 키워드 괄호·열거 항목·표 행, 고아·중복 앵커, 괄호 밖 키워드")
                .isEmpty();
    }

    @Test
    void knownRequirementsSitInTheExpectedSections() throws IOException {
        RequirementIndex index = index();
        assertRequirement(index, "req-ws-subprotocol-client-offer", "1.2.1", RequirementIndex.Keyword.MUST);
        assertRequirement(index, "req-status-reserved-unknown-recv", "2.5.1", RequirementIndex.Keyword.MUST);
        assertRequirement(index, "req-hello-send-independent-frames", "3.2.2", RequirementIndex.Keyword.MUST);
        assertRequirement(index, "req-close-last-stream-id-equals-highest", "3.5", RequirementIndex.Keyword.MUST);
        assertRequirement(index, "req-ping-ack-no-reply", "3.4", RequirementIndex.Keyword.MUST_NOT);
        assertRequirement(index, "req-ping-idle-send", "3.4.1", RequirementIndex.Keyword.SHOULD);
        assertRequirement(index, "req-check-new-request-order", "5.1", RequirementIndex.Keyword.MUST);
        assertRequirement(index, "req-security-tls-no-early-data", "6.1", RequirementIndex.Keyword.MUST);
    }

    @Test
    void sendAndRecvSidesAreSeparateRequirements() throws IOException {
        RequirementIndex index = index();
        assertThat(index.requirement("req-frame-reserved-zero-send").side()).isEqualTo(RequirementIndex.Side.SEND);
        assertThat(index.requirement("req-frame-reserved-ignore-recv").side()).isEqualTo(RequirementIndex.Side.RECV);
        assertThat(index.requirement("req-frame-reserved-zero-send").section()).isEqualTo("2.2.7");
        assertThat(index.requirement("req-frame-reserved-ignore-recv").section())
                .isEqualTo("2.2.7");
    }

    @Test
    void section51DefinesOnlyTheCheckOrderAndReactions() throws IOException {
        int inSection = 0;
        for (RequirementIndex.Requirement requirement : index().requirements()) {
            boolean inTable = requirement.section().equals("5.1");
            boolean checkId = requirement.id().startsWith("req-check-");
            assertThat(inTable)
                    .as("%s: 5.1절의 앵커는 req-check-이고, req-check-는 5.1절에만 있다", requirement.id())
                    .isEqualTo(checkId);
            if (inTable) {
                inSection++;
            }
        }
        assertThat(inSection).as("5.1절 표의 검사 순서·반응 앵커(행 1~6, 행 5는 반응 3개와 순서)").isEqualTo(11);
    }

    private static void assertRequirement(
            RequirementIndex index, String id, String section, RequirementIndex.Keyword keyword) {
        RequirementIndex.Requirement requirement = index.requirement(id);
        assertThat(requirement).as(id).isNotNull();
        assertThat(requirement.section()).as(id + " 절").isEqualTo(section);
        assertThat(requirement.keyword()).as(id + " 키워드").isEqualTo(keyword);
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
        assertThat(SpecCoverageTest.class.getResource("/deleted-requirement-ids.txt"))
                .as("deleted-requirement-ids.txt가 없다")
                .isNotNull();
        try (Reader reader = new InputStreamReader(
                        SpecCoverageTest.class.getResourceAsStream("/deleted-requirement-ids.txt"),
                        StandardCharsets.UTF_8);
                BufferedReader lines = new BufferedReader(reader)) {
            lines.lines()
                    .map(String::trim)
                    .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                    .forEach(result::add);
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
        assertThat(deletedRequirementIds())
                .allMatch(id -> Check.REQUIREMENT_ID.matcher(id).matches());
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
