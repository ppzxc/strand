package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * 번호를 받지 못한 대기열 요청과 전송 계층에 넘기기 전에 끝나는 요청의 결과 확정성, 그리고 PING 대기 제한이 규격에 한 절씩 정의되어 있는지 본다(#176). 벡터
 * 형식에는 대기열 요청과 전송 계층에 넘기는 시점이 없어 벡터로 확인할 수 없으므로 규격 본문을 검사한다.
 */
class QueuedRequestOutcomeSpecTest {

    private static final Path ROOT =
            VectorSuite.defaultDirectory().resolve("../..").normalize();
    private static final Path SPEC = ROOT.resolve("docs/specification/STRAND_SPECIFICATION.md");
    private static final Path UNCOVERED =
            ROOT.resolve("strand-conformance/src/test/resources/uncovered-spec-sections.txt");

    private static List<String> lines() throws IOException {
        return Files.readAllLines(SPEC, StandardCharsets.UTF_8);
    }

    /** 제목이 `heading`으로 시작하는 절의 본문을 다음 제목 직전까지 돌려준다. */
    private static String section(String heading) throws IOException {
        List<String> lines = lines();
        int start = -1;
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).startsWith("#") && lines.get(i).contains(heading)) {
                start = i + 1;
                break;
            }
        }
        assertThat(start).as("절 " + heading).isPositive();
        StringBuilder out = new StringBuilder();
        for (int i = start; i < lines.size() && !lines.get(i).startsWith("#"); i++) {
            out.append(lines.get(i)).append('\n');
        }
        return out.toString();
    }

    private static List<String> rows(String text) {
        return java.util.Arrays.stream(text.split("\n"))
                .filter(l -> l.startsWith("|"))
                .collect(Collectors.toList());
    }

    private static List<String> linesWith(String needle) throws IOException {
        return lines().stream().filter(l -> l.contains(needle)).collect(Collectors.toList());
    }

    @Test
    void glossaryDefinesQueuedRequest() throws IOException {
        assertThat(linesWith("**대기열 요청"))
                .as("1.3절 용어 정의")
                .singleElement()
                .asString()
                .contains("3.2.3절");
    }

    @Test
    void closeReceivedEndsQueuedRequestsAsNotProcessed() throws IOException {
        String close = section("3.5 연결 종료");
        assertThat(linesWith("CLOSE를 받으면 그 즉시"))
                .singleElement()
                .asString()
                .contains("대기열 요청마다")
                .contains("로컬 `status` = `UNAVAILABLE`(처리 안 됨)");
        assertThat(close).contains("대기열 요청마다");
        // 규칙 문장이 한 곳에만 있다.
        assertThat(linesWith("대기열 요청마다")).hasSize(1);
    }

    @Test
    void malformedCloseReadsByHandOverToTransport() throws IOException {
        String close = section("3.5 연결 종료");
        assertThat(close).contains("번호가 있고 전송 계층에 넘긴 요청은 결과 모름").contains("번호가 없거나 아직 넘기지 못한 요청은 처리 안 됨");
    }

    @Test
    void resultTableHasCancelBeforeHandOverRow() throws IOException {
        String reset = section("4.4 스트림 중단");
        assertThat(rows(reset).stream().filter(l -> l.contains("넘기기 전에")).collect(Collectors.toList()))
                .singleElement()
                .satisfies(row -> {
                    assertThat(row).contains("`CANCELLED`").contains("처리 안 됨").doesNotContain("결과 모름");
                });
        // 넘긴 뒤 취소는 결과 모름 그대로다.
        assertThat(rows(reset).stream().filter(l -> l.contains("넘긴 뒤")).collect(Collectors.toList()))
                .singleElement()
                .asString()
                .contains("`CANCELLED`")
                .contains("결과 모름");
    }

    @Test
    void queuedRequestPastDeadlineEndsLocallyWithoutReset() throws IOException {
        String deadline = section("4.4.1 요청 기한");
        assertThat(deadline).contains("대기열 요청").contains("`RESET(CANCEL)` 없이").contains("`DEADLINE_EXCEEDED`(처리 안 됨)");
    }

    @Test
    void idlePingIsLimitedToOneOutstanding() throws IOException {
        String idle = section("3.4.1 유휴 시 PING");
        assertThat(idle).contains("하나만").contains("(SHOULD)").contains("새로 보내지 않는다");
    }

    @Test
    void uncoveredReasonsNameTheUnobservableItems() throws IOException {
        List<String> uncovered = Files.readAllLines(UNCOVERED, StandardCharsets.UTF_8);
        assertThat(uncovered.stream().filter(l -> l.startsWith("4.4.1 ")).collect(Collectors.toList()))
                .singleElement()
                .asString()
                .contains("대기열");
        assertThat(uncovered.stream().filter(l -> l.startsWith("3.4.1 ")).collect(Collectors.toList()))
                .singleElement()
                .asString()
                .contains("PING");
    }
}
