package io.github.ppzxc.strand.conformance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    enum ProblemKind {
        ANCHORLESS_KEYWORD,

        ANCHORLESS_ITEM,

        ORPHAN_ANCHOR,

        KEYWORD_OUTSIDE_PARENTHESIS,
        DUPLICATE_ANCHOR,

        INVALID_ANCHOR,

        EMPTY_ENUMERATION
    }

    static final class Requirement {
        private final String id;
        private final String section;
        private final Keyword keyword;

        Requirement(String id, String section, Keyword keyword) {
            this.id = id;
            this.section = section;
            this.keyword = keyword;
        }

        String id() {
            return id;
        }

        String section() {
            return section;
        }

        Keyword keyword() {
            return keyword;
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

    private static final String KEYWORD_TEXT = "(?<![A-Za-z])(MUST NOT|SHOULD NOT|MUST|SHOULD)(?![A-Za-z])";

    private static final Pattern KEYWORD = Pattern.compile(KEYWORD_TEXT);

    private static final Pattern PARENTHESIS = Pattern.compile("\\((모두 )?" + KEYWORD_TEXT + "(?:,[^()]*)?\\)");

    private static final Pattern TOP_HEADING = Pattern.compile("^#{1,2}\\s");
    private static final Pattern HEADING_ANY = Pattern.compile("^#{1,6}\\s");
    private static final Pattern ANCHOR = Pattern.compile("<a id=\"((?i:req-)[^\"]*)\"></a>");
    private static final Pattern ANCHOR_RUN = Pattern.compile("(?:<a id=\"(?i:req-)[^\"]*\"></a>)+");
    private static final Pattern LIST_MARKER = Pattern.compile("^(?:[-*]|\\d+\\.)\\s.*");
    private static final Pattern TABLE_SEPARATOR = Pattern.compile("^\\|[\\s:|-]+\\|?$");

    private static final String KEYWORD_DEFINITION = "- MUST, MUST NOT";

    private final Map<String, Requirement> requirements = new LinkedHashMap<>();
    private final List<Problem> problems = new ArrayList<>();

    private RequirementIndex() {}

    static RequirementIndex parse(String markdown) {
        RequirementIndex index = new RequirementIndex();
        index.scan(markdown.split("\r?\n", -1));
        return index;
    }

    static final String SPEC_FILE_PROPERTY = "strand.conformance.spec.file";

    private static final Map<Path, RequirementIndex> BY_FILE = new HashMap<>();

    static synchronized RequirementIndex ofSpecification() {
        Path file = specificationFile();
        return BY_FILE.computeIfAbsent(file, RequirementIndex::readSpecification);
    }

    static Path specificationFile() {
        String property = System.getProperty(SPEC_FILE_PROPERTY);
        if (property != null) {
            return Paths.get(property).toAbsolutePath().normalize();
        }
        for (Path dir = Paths.get("").toAbsolutePath(); dir != null; dir = dir.getParent()) {
            Path candidate =
                    dir.resolve("docs/specification/STRAND_SPECIFICATION.md").normalize();
            if (Files.exists(candidate)) {
                return candidate;
            }
        }
        throw new IllegalStateException("규격 문서를 찾지 못했다. 시스템 프로퍼티 " + SPEC_FILE_PROPERTY + "를 준다");
    }

    static Path repositoryRoot() {
        Path docs = specificationFile().getParent().getParent();
        if (docs == null || !"docs".equals(String.valueOf(docs.getFileName()))) {
            throw new IllegalStateException("규격 문서가 docs/specification/ 아래에 있지 않다: " + specificationFile());
        }
        return docs.getParent();
    }

    private static RequirementIndex readSpecification(Path spec) {
        try {
            return parse(new String(Files.readAllBytes(spec), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Collection<Requirement> requirements() {
        return Collections.unmodifiableCollection(requirements.values());
    }

    Requirement requirement(String id) {
        return requirements.get(id);
    }

    List<String> sectionsOf(String owner, List<String> ids) {
        List<String> sections = new ArrayList<>();
        for (String id : ids) {
            Requirement found = requirements.get(id);
            if (found == null) {
                throw new AssertionError(owner + "의 요구사항 식별자가 규격에 없다: " + id);
            }
            sections.add(found.section());
        }
        return sections;
    }

    List<Problem> problems() {
        return Collections.unmodifiableList(problems);
    }

    private static final class Scan {

        final List<Bracket> brackets = new ArrayList<>();

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

    private static int skipFence(String[] lines, int open) {
        int i = open + 1;
        while (i < lines.length && !lines[i].startsWith("```")) {
            i++;
        }
        return Math.min(i + 1, lines.length);
    }

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
        requirements.put(id, new Requirement(id, section, keyword));
    }

    private void problem(ProblemKind kind, int index, String detail) {
        problems.add(new Problem(kind, index + 1, detail));
    }

    private Scan scanLine(String line) {
        Scan scan = new Scan();
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
