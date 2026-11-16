package io.github.ppzxc.strand.conformance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 규격 마크다운을 읽어 요구사항 목록과 앵커 규칙 위반을 돌려주는 도우미(GLOSSARY.md "요구사항"). 규격 문서를 파싱하는 곳은 여기 하나다.
 * "식별자 → 절·키워드·쪽" 조회는 {@link #requirement}만 거친다.
 *
 * <p>앵커 `<a id="req-…"></a>`가 놓일 수 있는 자리는 둘이다. (1) 키워드 괄호 `(MUST)` 바로 뒤. (2) 열거 줄(`(모두 MUST)`) 바로 뒤
 * 목록의 최상위 항목과 표의 행. 그 밖의 앵커는 고아다. 위반은 {@link #problems}로 모아 돌려주고 던지지 않는다. 호출하는 검사가 실패로
 * 바꾼다.
 */
final class RequirementIndex {

    enum Keyword {
        MUST("MUST", true),
        MUST_NOT("MUST NOT", true),
        SHOULD("SHOULD", false),
        SHOULD_NOT("SHOULD NOT", false);

        private final String text;
        private final boolean mandatory;

        Keyword(String text, boolean mandatory) {
            this.text = text;
            this.mandatory = mandatory;
        }

        /** MUST·MUST NOT이면 true. 벡터가 확인해야 하는 키워드다. */
        boolean mandatory() {
            return mandatory;
        }

        static Keyword of(String text) {
            for (Keyword keyword : values()) {
                if (keyword.text.equals(text)) {
                    return keyword;
                }
            }
            throw new IllegalArgumentException(text);
        }
    }

    enum Side {
        SEND,
        RECV,
        NONE
    }

    enum ProblemKind {
        /** 키워드 괄호 바로 뒤에 앵커가 없다. */
        ANCHORLESS_KEYWORD,
        /** 열거 항목이나 표 행에 앵커가 없다. */
        ANCHORLESS_ITEM,
        /** 놓일 수 없는 자리의 앵커. */
        ORPHAN_ANCHOR,
        /** 키워드가 괄호 밖에 있다. */
        KEYWORD_OUTSIDE_PARENTHESIS,
        DUPLICATE_ANCHOR,
        /** `req-`로 시작하지만 식별자 형식이 아니다. */
        INVALID_ANCHOR,
        /** 열거 줄 뒤에 목록이나 표가 없다. */
        EMPTY_ENUMERATION
    }

    static final class Requirement {
        private final String id;
        private final String section;
        private final Keyword keyword;
        private final Side side;

        Requirement(String id, String section, Keyword keyword, Side side) {
            this.id = id;
            this.section = section;
            this.keyword = keyword;
            this.side = side;
        }

        String id() {
            return id;
        }

        /** 앵커가 놓인 자리의 절 번호. */
        String section() {
            return section;
        }

        Keyword keyword() {
            return keyword;
        }

        /** 식별자가 `-send`/`-recv`로 끝나면 그 쪽, 아니면 {@link Side#NONE}. */
        Side side() {
            return side;
        }
    }

    static final class Problem {
        private final ProblemKind kind;
        private final int line;
        private final String detail;

        Problem(ProblemKind kind, int line, String detail) {
            this.kind = kind;
            this.line = line;
            this.detail = detail;
        }

        ProblemKind kind() {
            return kind;
        }

        /** 1부터 세는 줄 번호. */
        int line() {
            return line;
        }

        String detail() {
            return detail;
        }

        @Override
        public String toString() {
            return kind + " " + line + "행: " + detail;
        }
    }

    private static final Pattern HEADING = Pattern.compile("^#{2,4} (\\d+(?:\\.\\d+)*)\\.?(?:\\s|$)");
    /** {@code \b}는 JDK 19부터 한글을 단어 문자로 보지 않아 JDK마다 결과가 달랐다. 영문자 기준 lookaround로 고정한다. */
    private static final String KEYWORD_TEXT = "(?<![A-Za-z])(MUST NOT|SHOULD NOT|MUST|SHOULD)(?![A-Za-z])";

    private static final Pattern KEYWORD = Pattern.compile(KEYWORD_TEXT);
    /** `(MUST)`, `(MUST NOT, 5.1절)`, `(모두 MUST)`. 괄호 안은 키워드로 시작한다. */
    private static final Pattern PARENTHESIS = Pattern.compile("\\((모두 )?" + KEYWORD_TEXT + "(?:,[^()]*)?\\)");

    private static final Pattern TOP_HEADING = Pattern.compile("^#{1,2}\\s");
    private static final Pattern HEADING_ANY = Pattern.compile("^#{1,6}\\s");
    private static final Pattern ANCHOR = Pattern.compile("<a id=\"((?i:req-)[^\"]*)\"></a>");
    private static final Pattern ANCHOR_RUN = Pattern.compile("(?:<a id=\"(?i:req-)[^\"]*\"></a>)+");
    private static final Pattern LIST_MARKER = Pattern.compile("^(?:[-*]|\\d+\\.)\\s.*");
    private static final Pattern TABLE_SEPARATOR = Pattern.compile("^\\|[\\s:|-]+\\|?$");
    /** 용어 규칙을 설명하는 줄이라 규범 문장이 아니다. */
    private static final String KEYWORD_DEFINITION = "- MUST, MUST NOT";

    private final Map<String, Integer> keywordSections = new TreeMap<>();
    private final Map<String, Requirement> requirements = new LinkedHashMap<>();
    private final List<Problem> problems = new ArrayList<>();

    private RequirementIndex() {}

    static RequirementIndex parse(String markdown) {
        RequirementIndex index = new RequirementIndex();
        index.scan(markdown.split("\r?\n", -1));
        return index;
    }

    /** 저장소의 실제 규격 문서를 읽어 만든 색인. */
    static RequirementIndex ofSpecification() {
        Path spec = VectorSuite.defaultDirectory()
                .resolve("../../docs/specification/STRAND_SPECIFICATION.md")
                .normalize();
        try {
            return parse(new String(Files.readAllBytes(spec), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 규범 키워드가 있는 절 번호와 그 줄 수(앵커 유무와 무관하다). */
    Map<String, Integer> keywordSections() {
        return keywordSections;
    }

    /** 앵커 순서대로의 요구사항 목록. */
    List<Requirement> requirements() {
        return Collections.unmodifiableList(new ArrayList<>(requirements.values()));
    }

    /** 식별자로 찾은 요구사항. 없으면 null. */
    Requirement requirement(String id) {
        return requirements.get(id);
    }

    List<Problem> problems() {
        return Collections.unmodifiableList(problems);
    }

    // ---- 파싱 ----

    /** 한 줄을 훑은 결과. */
    private static final class Scan {
        /** 키워드 괄호와, 그 바로 뒤에 붙은 앵커 목록. */
        final List<Bracket> brackets = new ArrayList<>();
        /** 키워드 괄호 바로 뒤가 아닌 자리의 앵커. */
        final List<String> looseAnchors = new ArrayList<>();

        int outsideKeywords;
    }

    private static final class Bracket {
        final Keyword keyword;
        final boolean lead;
        final List<String> anchors = new ArrayList<>();

        Bracket(Keyword keyword, boolean lead) {
            this.keyword = keyword;
            this.lead = lead;
        }
    }

    private String section;

    private void scan(String[] lines) {
        int i = 0;
        while (i < lines.length) {
            String line = lines[i];
            Matcher heading = HEADING.matcher(line);
            if (heading.find()) {
                section = heading.group(1);
                i++;
            } else if (TOP_HEADING.matcher(line).find()) {
                // 번호 없는 2단계 제목(부록 등)은 앞 절에 속하지 않는다.
                section = null;
                i++;
            } else if (line.startsWith("```")) {
                i = skipFence(lines, i);
            } else if (HEADING_ANY.matcher(line).find() || line.startsWith(KEYWORD_DEFINITION)) {
                i++;
            } else {
                i = scanBlock(lines, i);
            }
        }
    }

    /** 코드 블록(```)은 규격 문장이 아니므로 건너뛴다. 닫는 줄 다음 줄 번호를 돌려준다. */
    private static int skipFence(String[] lines, int open) {
        int i = open + 1;
        while (i < lines.length && !lines[i].startsWith("```")) {
            i++;
        }
        return Math.min(i + 1, lines.length);
    }

    /** 일반 줄 하나를 처리한다. 열거 줄이면 이어지는 목록·표까지 처리하고 다음에 읽을 줄 번호를 돌려준다. */
    private int scanBlock(String[] lines, int index) {
        Keyword lead = scanPlain(lines[index], index);
        if (lead == null) {
            return index + 1;
        }
        return scanEnumeration(lines, index + 1, lead);
    }

    private int scanEnumeration(String[] lines, int start, Keyword lead) {
        int i = start;
        while (i < lines.length && lines[i].trim().isEmpty()) {
            i++;
        }
        if (i < lines.length && lines[i].trim().startsWith("|")) {
            return scanTable(lines, i, lead);
        }
        if (i < lines.length && LIST_MARKER.matcher(lines[i]).matches()) {
            return scanList(lines, i, lead);
        }
        problem(ProblemKind.EMPTY_ENUMERATION, start - 1, "열거 줄 뒤에 목록이나 표가 없다");
        return start;
    }

    private int scanTable(String[] lines, int start, Keyword lead) {
        int i = start;
        int row = 0;
        while (i < lines.length && lines[i].trim().startsWith("|")) {
            String line = lines[i].trim();
            if (row == 1 && TABLE_SEPARATOR.matcher(line).matches()) {
                row++;
                i++;
                continue;
            }
            if (row == 0) {
                scanPlain(lines[i], i);
            } else {
                scanItem(new String[] {lines[i]}, i, lead);
            }
            row++;
            i++;
        }
        return i;
    }

    private int scanList(String[] lines, int start, Keyword lead) {
        int i = start;
        while (i < lines.length && LIST_MARKER.matcher(lines[i]).matches()) {
            int end = i + 1;
            while (end < lines.length && continuesItem(lines, end)) {
                end++;
            }
            // 항목 끝의 빈 줄은 항목에 넣지 않는다.
            int last = end;
            while (last > i + 1 && lines[last - 1].trim().isEmpty()) {
                last--;
            }
            String[] item = new String[last - i];
            System.arraycopy(lines, i, item, 0, item.length);
            scanItem(item, i, lead);
            i = end;
        }
        return i;
    }

    /** 최상위 항목 뒤의 이 줄이 같은 항목에 속하는가: 들여쓴 줄, 또는 들여쓴 줄이 이어지는 빈 줄. */
    private static boolean continuesItem(String[] lines, int index) {
        String line = lines[index];
        if (line.trim().isEmpty()) {
            int next = index + 1;
            while (next < lines.length && lines[next].trim().isEmpty()) {
                next++;
            }
            return next < lines.length
                    && lines[next].startsWith(" ")
                    && !lines[next].trim().isEmpty();
        }
        return line.startsWith(" ") || line.startsWith("\t");
    }

    /** 열거 줄이 아닌 줄(또는 열거 줄 자체)을 검사한다. 열거 줄이면 그 키워드를 돌려준다. */
    private Keyword scanPlain(String line, int index) {
        Scan scan = scanLine(line);
        Keyword lead = null;
        for (Bracket bracket : scan.brackets) {
            if (bracket.lead) {
                lead = bracket.keyword;
                for (String anchor : bracket.anchors) {
                    problem(ProblemKind.ORPHAN_ANCHOR, index, "열거 줄 뒤에는 앵커를 두지 않는다: " + anchor);
                }
            } else {
                acceptBracket(bracket, index);
            }
        }
        for (String anchor : scan.looseAnchors) {
            problem(ProblemKind.ORPHAN_ANCHOR, index, "앵커가 놓일 수 없는 자리다: " + anchor);
        }
        record(scan, index);
        return lead;
    }

    /** 열거 항목(여러 줄) 또는 표 행 하나. 자기 키워드 괄호 뒤 앵커는 그 키워드, 나머지 앵커는 이끄는 줄의 키워드다. */
    private void scanItem(String[] itemLines, int firstIndex, Keyword lead) {
        int anchors = 0;
        boolean reported = false;
        for (int k = 0; k < itemLines.length; k++) {
            int index = firstIndex + k;
            Scan scan = scanLine(itemLines[k]);
            for (Bracket bracket : scan.brackets) {
                if (bracket.lead) {
                    for (String anchor : bracket.anchors) {
                        problem(ProblemKind.ORPHAN_ANCHOR, index, "열거 줄 뒤에는 앵커를 두지 않는다: " + anchor);
                    }
                } else {
                    acceptBracket(bracket, index);
                    anchors += bracket.anchors.size();
                    reported |= bracket.anchors.isEmpty();
                }
            }
            for (String anchor : scan.looseAnchors) {
                add(anchor, lead, index);
                anchors++;
            }
            record(scan, index);
        }
        if (anchors == 0 && !reported) {
            problem(ProblemKind.ANCHORLESS_ITEM, firstIndex, "열거 항목·표 행에 앵커가 없다");
        }
    }

    private void acceptBracket(Bracket bracket, int index) {
        if (bracket.anchors.isEmpty()) {
            problem(ProblemKind.ANCHORLESS_KEYWORD, index, "키워드 괄호 뒤에 앵커가 없다: " + bracket.keyword);
        }
        for (String anchor : bracket.anchors) {
            add(anchor, bracket.keyword, index);
        }
    }

    /** 괄호 밖 키워드를 보고한다. */
    private void record(Scan scan, int index) {
        for (int k = 0; k < scan.outsideKeywords; k++) {
            problem(ProblemKind.KEYWORD_OUTSIDE_PARENTHESIS, index, "키워드가 괄호 밖에 있다");
        }
    }

    private void add(String id, Keyword keyword, int index) {
        if (!Check.REQUIREMENT_ID.matcher(id).matches()) {
            problem(ProblemKind.INVALID_ANCHOR, index, "식별자 형식이 아니다: " + id);
            return;
        }
        if (section == null) {
            problem(ProblemKind.ORPHAN_ANCHOR, index, "절 제목 앞의 앵커다: " + id);
            return;
        }
        if (requirements.containsKey(id)) {
            problem(ProblemKind.DUPLICATE_ANCHOR, index, "앵커가 두 번 나온다: " + id);
            return;
        }
        Side side = id.endsWith("-send") ? Side.SEND : id.endsWith("-recv") ? Side.RECV : Side.NONE;
        requirements.put(id, new Requirement(id, section, keyword, side));
    }

    private void problem(ProblemKind kind, int index, String detail) {
        problems.add(new Problem(kind, index + 1, detail));
    }

    /** 한 줄의 키워드 괄호·앵커·괄호 밖 키워드를 찾는다. 절 통계도 여기서 센다. */
    private Scan scanLine(String line) {
        Scan scan = new Scan();
        if (section != null && KEYWORD.matcher(line).find()) {
            keywordSections.merge(section, 1, Integer::sum);
        }
        StringBuilder rest = new StringBuilder(line);
        boolean[] claimed = new boolean[line.length()];
        Matcher parenthesis = PARENTHESIS.matcher(line);
        while (parenthesis.find()) {
            Bracket bracket = new Bracket(Keyword.of(parenthesis.group(2)), parenthesis.group(1) != null);
            blank(rest, parenthesis.start(), parenthesis.end());
            int end = parenthesis.end();
            Matcher run = ANCHOR_RUN.matcher(line).region(end, line.length());
            if (run.lookingAt()) {
                Matcher anchor = ANCHOR.matcher(run.group());
                while (anchor.find()) {
                    bracket.anchors.add(anchor.group(1));
                }
                for (int p = run.start(); p < run.end(); p++) {
                    claimed[p] = true;
                }
            }
            scan.brackets.add(bracket);
        }
        Matcher anchor = ANCHOR.matcher(line);
        while (anchor.find()) {
            if (!claimed[anchor.start()]) {
                scan.looseAnchors.add(anchor.group(1));
            }
        }
        Matcher outside = KEYWORD.matcher(rest);
        while (outside.find()) {
            scan.outsideKeywords++;
        }
        return scan;
    }

    private static void blank(StringBuilder text, int start, int end) {
        for (int p = start; p < end; p++) {
            text.setCharAt(p, ' ');
        }
    }
}
