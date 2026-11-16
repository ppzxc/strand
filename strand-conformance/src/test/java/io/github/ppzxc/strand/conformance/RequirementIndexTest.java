package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 작은 가짜 규격으로 요구사항 색인 도우미의 외부 행동(요구사항 목록과 문제 보고)을 확인한다. */
class RequirementIndexTest {

    private static String spec(String... lines) {
        return String.join("\n", lines);
    }

    private static RequirementIndex.Requirement only(RequirementIndex index, String id) {
        RequirementIndex.Requirement found = index.requirement(id);
        assertThat(found).as(id).isNotNull();
        return found;
    }

    private static List<String> kinds(RequirementIndex index) {
        List<String> kinds = new ArrayList<>();
        for (RequirementIndex.Problem problem : index.problems()) {
            kinds.add(problem.kind().name());
        }
        return kinds;
    }

    @Test
    void anchorAfterKeywordParenthesisInALine() {
        RequirementIndex index = RequirementIndex.parse(
                spec("## 2. 제목", "", "### 2.1 절", "", "보낸다(MUST)<a id=\"req-a-b\"></a>. 그 밖의 말.", ""));

        RequirementIndex.Requirement r = only(index, "req-a-b");
        assertThat(r.section()).isEqualTo("2.1");
        assertThat(r.keyword()).isEqualTo(RequirementIndex.Keyword.MUST);
        assertThat(index.problems()).isEmpty();
    }

    @Test
    void keywordParenthesisMayCarryASectionReference() {
        RequirementIndex index = RequirementIndex.parse(spec("## 5. 절", "연결 오류다(MUST, 5.1절)<a id=\"req-x\"></a>."));

        assertThat(only(index, "req-x").keyword()).isEqualTo(RequirementIndex.Keyword.MUST);
        assertThat(index.problems()).isEmpty();
    }

    @Test
    void anchorsInATableCellAfterTheKeywordParenthesis() {
        RequirementIndex index = RequirementIndex.parse(
                spec("## 3. 절", "", "| 조건 | 반응 |", "|---|---|", "| 하나 | 닫는다(MUST)<a id=\"req-cell-close\"></a> |"));

        assertThat(only(index, "req-cell-close").keyword()).isEqualTo(RequirementIndex.Keyword.MUST);
        assertThat(index.problems()).isEmpty();
    }

    @Test
    void sendAndRecvAnchorsSideBySideGiveOneRequirementPerSide() {
        RequirementIndex index = RequirementIndex.parse(
                spec("## 3. 절", "0으로 보내고 받으면 무시한다(MUST)<a id=\"req-z-send\"></a><a id=\"req-z-recv\"></a>."));

        assertThat(index.requirements()).extracting(r -> r.id()).containsExactly("req-z-send", "req-z-recv");
        assertThat(index.problems()).isEmpty();
    }

    @Test
    void enumerationItemsInheritTheLeadKeywordUnlessTheyHaveTheirOwn() {
        RequirementIndex index = RequirementIndex.parse(spec(
                "## 3. 절",
                "",
                "송신 규칙은 다음과 같다(모두 MUST).",
                "",
                "1. 첫째<a id=\"req-first\"></a>",
                "2. 둘째 하위는 지켜도 되고(SHOULD)<a id=\"req-second-should\"></a> 나머지는 본다.<a id=\"req-second\"></a>",
                "   - 이어지는 줄 <a id=\"req-nested\"></a>",
                "",
                "문단이 끝난다."));

        assertThat(only(index, "req-first").keyword()).isEqualTo(RequirementIndex.Keyword.MUST);
        assertThat(only(index, "req-second").keyword()).isEqualTo(RequirementIndex.Keyword.MUST);
        assertThat(only(index, "req-nested").keyword()).isEqualTo(RequirementIndex.Keyword.MUST);
        assertThat(only(index, "req-second-should").keyword()).isEqualTo(RequirementIndex.Keyword.SHOULD);
        assertThat(index.problems()).isEmpty();
    }

    @Test
    void enumerationTableRowsHaveAnchorsAndInheritTheKeyword() {
        RequirementIndex index = RequirementIndex.parse(spec(
                "## 5. 절",
                "표의 반응은 모두 다음과 같다(모두 MUST NOT).",
                "",
                "| 순서 | 반응 |",
                "|---|---|",
                "| 1 | 하지 않는다<a id=\"req-row-one\"></a> |",
                "| 2 | 하지 않는다 |"));

        assertThat(only(index, "req-row-one").keyword()).isEqualTo(RequirementIndex.Keyword.MUST_NOT);
        assertThat(kinds(index)).containsExactly("ANCHORLESS_ITEM");
    }

    @Test
    void keywordParenthesisWithoutAnchorIsReported() {
        RequirementIndex index = RequirementIndex.parse(spec("## 2. 절", "보낸다(MUST). 앵커가 없다."));

        assertThat(kinds(index)).containsExactly("ANCHORLESS_KEYWORD");
        assertThat(index.requirements()).isEmpty();
    }

    @Test
    void enumerationItemWithoutAnchorIsReported() {
        RequirementIndex index = RequirementIndex.parse(spec(
                "## 2. 절",
                "다음은 모두 오류다(모두 MUST).",
                "",
                "- 하나<a id=\"req-one\"></a>",
                "- 둘",
                "- 셋<a id=\"req-three\"></a>"));

        assertThat(kinds(index)).containsExactly("ANCHORLESS_ITEM");
        assertThat(index.problems().get(0).line()).isEqualTo(5);
    }

    @Test
    void enumerationWithoutItemsIsReported() {
        RequirementIndex index = RequirementIndex.parse(spec("## 2. 절", "다음은 모두 오류다(모두 MUST).", "", "그냥 문단."));

        assertThat(kinds(index)).containsExactly("EMPTY_ENUMERATION");
    }

    @Test
    void anchorOutsideAnyAllowedPlaceIsAnOrphan() {
        RequirementIndex index = RequirementIndex.parse(
                spec("## 2. 절", "키워드 없는 문장<a id=\"req-orphan\"></a>.", "열거 줄(모두 MUST)<a id=\"req-lead\"></a>"));

        assertThat(kinds(index)).contains("ORPHAN_ANCHOR");
        assertThat(index.problems()).extracting(p -> p.detail()).anyMatch(d -> d.contains("req-orphan"));
        assertThat(index.problems()).extracting(p -> p.detail()).anyMatch(d -> d.contains("req-lead"));
    }

    @Test
    void keywordOutsideParenthesisIsReported() {
        RequirementIndex index =
                RequirementIndex.parse(spec("## 2. 절", "표의 반응은 모두 MUST다.", "한글MUST다. 그리고 (MUST)<a id=\"req-ok\"></a>"));

        assertThat(kinds(index)).containsExactly("KEYWORD_OUTSIDE_PARENTHESIS", "KEYWORD_OUTSIDE_PARENTHESIS");
        assertThat(index.requirements()).hasSize(1);
    }

    @Test
    void duplicateAnchorIsReported() {
        RequirementIndex index = RequirementIndex.parse(
                spec("## 2. 절", "하나(MUST)<a id=\"req-dup\"></a>.", "둘(MUST)<a id=\"req-dup\"></a>."));

        assertThat(kinds(index)).containsExactly("DUPLICATE_ANCHOR");
        assertThat(index.requirements()).hasSize(1);
    }

    @Test
    void anchorIdWithABadShapeIsReported() {
        RequirementIndex index = RequirementIndex.parse(spec("## 2. 절", "하나(MUST)<a id=\"req-Bad_Name\"></a>."));

        assertThat(kinds(index)).containsExactly("INVALID_ANCHOR");
    }

    @Test
    void termDefinitionLineIsNotNormative() {
        RequirementIndex index = RequirementIndex.parse(
                spec("## 1. 절", "- MUST, MUST NOT, SHOULD, SHOULD NOT, MAY는 대문자로 쓴 경우에만 요구 수준을 뜻한다."));

        assertThat(index.problems()).isEmpty();
    }

    @Test
    void headingsFromLevelTwoToFourAttributeAnchorsToTheirSection() {
        RequirementIndex index = RequirementIndex.parse(spec(
                "# 문서",
                "## 2. 가",
                "가(MUST)<a id=\"req-in-2\"></a>",
                "### 2.1 나",
                "나(MUST)<a id=\"req-in-21\"></a>",
                "#### 2.1.3. 다",
                "다(MUST)<a id=\"req-in-213\"></a>",
                "## 3 라",
                "라(MUST)<a id=\"req-in-3\"></a>"));

        assertThat(only(index, "req-in-2").section()).isEqualTo("2");
        assertThat(only(index, "req-in-21").section()).isEqualTo("2.1");
        assertThat(only(index, "req-in-213").section()).isEqualTo("2.1.3");
        assertThat(only(index, "req-in-3").section()).isEqualTo("3");
    }

    @Test
    void mustNotAndShouldNotAreToldApartFromMustAndShould() {
        RequirementIndex index = RequirementIndex.parse(spec(
                "## 2. 절",
                "a(MUST NOT)<a id=\"req-a\"></a>",
                "b(SHOULD NOT)<a id=\"req-b\"></a>",
                "c(SHOULD)<a id=\"req-c\"></a>",
                "d(MUST)<a id=\"req-d\"></a>"));

        assertThat(only(index, "req-a").keyword()).isEqualTo(RequirementIndex.Keyword.MUST_NOT);
        assertThat(only(index, "req-b").keyword()).isEqualTo(RequirementIndex.Keyword.SHOULD_NOT);
        assertThat(only(index, "req-c").keyword()).isEqualTo(RequirementIndex.Keyword.SHOULD);
        assertThat(only(index, "req-d").keyword()).isEqualTo(RequirementIndex.Keyword.MUST);
        assertThat(only(index, "req-a").keyword().mandatory()).isTrue();
        assertThat(only(index, "req-b").keyword().mandatory()).isFalse();
    }

    @Test
    void keywordRightNextToHangulIsRecognized() {
        RequirementIndex index = RequirementIndex.parse(
                spec("## 2. 절", "보내야한다(MUST)<a id=\"req-h\"></a>다.", "받는다(SHOULD)<a id=\"req-h2\"></a>"));

        assertThat(only(index, "req-h").keyword()).isEqualTo(RequirementIndex.Keyword.MUST);
        assertThat(only(index, "req-h2").keyword()).isEqualTo(RequirementIndex.Keyword.SHOULD);
    }

    @Test
    void fencedCodeAndAnchorsBeforeTheFirstHeadingAreHandled() {
        RequirementIndex index = RequirementIndex.parse(spec(
                "앞말(MUST)<a id=\"req-pre\"></a>",
                "## 2. 절",
                "```",
                "MUST (MUST)",
                "```",
                "끝(MUST)<a id=\"req-end\"></a>"));

        assertThat(kinds(index)).containsExactly("ORPHAN_ANCHOR");
        assertThat(index.requirement("req-end")).isNotNull();
        assertThat(index.requirement("req-pre")).isNull();
    }

    @Test
    void unnumberedHeadingEndsTheSection() {
        RequirementIndex index = RequirementIndex.parse(
                spec("## 2. 절", "## 부록 A. 매핑", "끝(MUST)<a id=\"req-appendix\"></a>", "다시 말(MUST)"));

        assertThat(index.requirement("req-appendix")).isNull();
    }

    @Test
    void deeperUnnumberedHeadingsKeepTheSectionAndCrlfIsAccepted() {
        RequirementIndex index = RequirementIndex.parse(
                "## 5. 절\r\n##### 비고\r\n다음은 모두 오류다(모두 MUST).\r\n\r\n- 하나<a id=\"req-one\"></a>\r\n");

        assertThat(index.problems()).isEmpty();
        assertThat(index.requirement("req-one").section()).isEqualTo("5");
    }

    @Test
    void uppercasePrefixAnchorIsAnInvalidAnchorNotAMissingOne() {
        RequirementIndex index = RequirementIndex.parse(spec("## 2. 절", "하나(MUST)<a id=\"Req-foo\"></a>."));

        assertThat(kinds(index)).containsExactly("INVALID_ANCHOR");
    }
}
