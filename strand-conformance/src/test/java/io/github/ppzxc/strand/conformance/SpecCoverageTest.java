package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * 규격과 벡터의 대응을 요구사항 단위로 강제한다. MUST·MUST NOT 요구사항은 어떤 벡터의 {@code requirements}가 가리키거나, {@code
 * uncovered-requirements.txt}에 범주와 사유를 붙여 올라 있어야 한다. SHOULD·SHOULD NOT 요구사항은 커버 의무가 없고 벡터가 가리키면 실패한다.
 */
class SpecCoverageTest {

    /** 벡터도 제외 목록도 가리키지 않는 MUST·MUST NOT 요구사항. */
    private static List<String> uncoveredMandatory(
            RequirementIndex index, Map<String, List<String>> byVector, UncoveredRequirements exclusions) {
        Set<String> referenced = RawVectors.referencedIn(byVector);
        List<String> result = new ArrayList<>();
        for (RequirementIndex.Requirement requirement : index.requirements()) {
            if (requirement.keyword().mandatory()
                    && !referenced.contains(requirement.id())
                    && !exclusions.ids().contains(requirement.id())) {
                result.add(requirement.id());
            }
        }
        return result;
    }

    /** 제외 목록 줄의 오류: 형식·범주, 규격에 없는 식별자, MUST·MUST NOT이 아닌 식별자, 벡터가 가리키는 식별자. */
    private static List<String> exclusionProblems(
            RequirementIndex index, Map<String, List<String>> byVector, UncoveredRequirements exclusions) {
        List<String> problems = new ArrayList<>(exclusions.problems());
        Set<String> referenced = RawVectors.referencedIn(byVector);
        for (String id : exclusions.ids()) {
            RequirementIndex.Requirement requirement = index.requirement(id);
            if (requirement == null) {
                problems.add("제외 목록의 식별자가 규격에 없다: " + id);
            } else if (!requirement.keyword().mandatory()) {
                problems.add("제외 목록에 " + requirement.keyword() + " 요구사항이 있다: " + id);
            }
            if (referenced.contains(id)) {
                problems.add("제외 목록의 식별자를 벡터가 가리킨다. 줄을 지운다: " + id);
            }
        }
        return problems;
    }

    @Test
    void everyMandatoryRequirementIsCoveredByAVectorOrExcluded() throws IOException {
        assertThat(uncoveredMandatory(index(), RawVectors.requirementsByKey(), UncoveredRequirements.load()))
                .as("벡터가 가리키지 않는 MUST·MUST NOT 요구사항. 벡터를 추가하거나 uncovered-requirements.txt에 범주와 사유를 붙여 올린다")
                .isEmpty();
    }

    @Test
    void exclusionsAreWellFormedMandatoryRequirementsNoVectorCovers() throws IOException {
        assertThat(exclusionProblems(index(), RawVectors.requirementsByKey(), UncoveredRequirements.load()))
                .isEmpty();
    }

    @Test
    void uncoveredMandatoryRequirementIsNamedInTheFailure() {
        RequirementIndex index = RequirementIndex.parse("## 2. 절\n"
                + "하나(MUST)<a id=\"req-cov\"></a>\n"
                + "둘(MUST NOT)<a id=\"req-excl\"></a>\n"
                + "셋(MUST)<a id=\"req-miss\"></a>\n"
                + "넷(SHOULD)<a id=\"req-soft\"></a>\n");
        Map<String, List<String>> vectors = new TreeMap<>();
        vectors.put("f.json/a", Arrays.asList("req-cov"));
        UncoveredRequirements exclusions = UncoveredRequirements.parse(Arrays.asList("req-excl 관측불가 사유"));

        assertThat(uncoveredMandatory(index, vectors, exclusions)).containsExactly("req-miss");
    }

    @Test
    void badExclusionLinesAreNamedInTheFailure() {
        RequirementIndex index = RequirementIndex.parse("## 2. 절\n"
                + "하나(MUST)<a id=\"req-cov\"></a>\n"
                + "둘(MUST)<a id=\"req-ok\"></a>\n"
                + "셋(SHOULD)<a id=\"req-soft\"></a>\n");
        Map<String, List<String>> vectors = new TreeMap<>();
        vectors.put("f.json/a", Arrays.asList("req-cov"));
        UncoveredRequirements exclusions = UncoveredRequirements.parse(Arrays.asList(
                "# 주석",
                "",
                "req-ok 관측불가 정상",
                "req-nocategory 사유만 있고 범주 표지가 없다",
                "req-missing-reason 형식미지원",
                "req-ghost 관측불가 규격에 없다",
                "req-soft 관측불가 SHOULD다",
                "req-cov 형식미지원 벡터가 가리킨다",
                "req-ok 관측불가 두 번 올랐다"));

        assertThat(exclusionProblems(index, vectors, exclusions))
                .hasSize(6)
                .anyMatch(p -> p.contains("req-nocategory") && p.contains("범주"))
                .anyMatch(p -> p.contains("req-missing-reason"))
                .anyMatch(p -> p.contains("req-ghost") && p.contains("규격에 없다"))
                .anyMatch(p -> p.contains("req-soft") && p.contains("SHOULD"))
                .anyMatch(p -> p.contains("req-cov") && p.contains("벡터가 가리킨다"))
                .anyMatch(p -> p.contains("req-ok") && p.contains("두 번"));
    }

    @Test
    void exclusionsUseOnlyTheTwoCategories() throws IOException {
        UncoveredRequirements exclusions = UncoveredRequirements.load();
        for (String id : exclusions.ids()) {
            assertThat(exclusions.category(id)).as(id).isIn(UncoveredRequirements.CATEGORIES);
            assertThat(exclusions.reason(id)).as(id).isNotBlank();
        }
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
    void sendAndRecvAnchorsAreSeparateRequirements() throws IOException {
        RequirementIndex index = index();
        // 보내는 쪽과 받는 쪽은 식별자 접미사(-send/-recv)로 갈리는 별개의 요구사항이다.
        List<String> pair = new ArrayList<>();
        for (RequirementIndex.Requirement requirement : index.requirements()) {
            if (requirement.id().startsWith("req-frame-reserved-")) {
                pair.add(requirement.id());
            }
        }
        assertThat(pair).containsExactlyInAnyOrder("req-frame-reserved-zero-send", "req-frame-reserved-ignore-recv");
        assertThat(pair).filteredOn(id -> id.endsWith("-send")).hasSize(1);
        assertThat(pair).filteredOn(id -> id.endsWith("-recv")).hasSize(1);
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

    // ---- 벡터의 requirements ----

    private static RequirementIndex index() {
        return RequirementIndex.ofSpecification();
    }

    private static List<String> deletedRequirementIds() throws IOException {
        return UncoveredRequirements.dataLines("/deleted-requirement-ids.txt");
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
            RequirementIndex index,
            Map<String, List<String>> byVector,
            UncoveredRequirements exclusions,
            List<String> deleted) {
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
            if (exclusions.ids().contains(id)) {
                problems.add("지운 식별자가 제외 목록에 나온다: " + id);
            }
        }
        return problems;
    }

    @Test
    void vectorsPointOnlyToExistingMandatoryRequirements() throws IOException {
        assertThat(unknownReferences(index(), RawVectors.requirementsByKey())).isEmpty();
        assertThat(shouldReferences(index(), RawVectors.requirementsByKey())).isEmpty();
    }

    @Test
    void deletedRequirementIdsDoNotReturn() throws IOException {
        assertThat(deletedRequirementIds())
                .allMatch(id -> Check.REQUIREMENT_ID.matcher(id).matches());
        assertThat(revivedDeletedIds(
                        index(), RawVectors.requirementsByKey(), UncoveredRequirements.load(), deletedRequirementIds()))
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
    void deletedNameInASpecAnchorVectorOrExclusionFails() {
        RequirementIndex index = RequirementIndex.parse("## 2. 절\n보낸다(MUST)<a id=\"req-old\"></a>\n");
        Map<String, List<String>> vectors = new TreeMap<>();
        vectors.put("f.json/a", Arrays.asList("req-gone"));

        assertThat(revivedDeletedIds(
                        index,
                        vectors,
                        UncoveredRequirements.parse(Arrays.asList("req-listed 형식미지원 사유")),
                        Arrays.asList("req-old", "req-gone", "req-never", "req-listed")))
                .hasSize(3)
                .anyMatch(p -> p.contains("req-listed") && p.contains("제외 목록"))
                .anyMatch(p -> p.contains("req-old"))
                .anyMatch(p -> p.contains("req-gone") && p.contains("f.json/a"));
    }
}
