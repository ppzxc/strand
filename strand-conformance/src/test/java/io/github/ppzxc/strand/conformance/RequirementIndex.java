package io.github.ppzxc.strand.conformance;

import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 규격 마크다운을 읽어 절 제목과 규범 키워드가 있는 절을 돌려주는 도우미. 규격 문서를 파싱하는 곳은 여기 하나다. */
final class RequirementIndex {

    private static final Pattern HEADING = Pattern.compile("^#{2,4} (\\d+(?:\\.\\d+)*)\\.?\\s");
    /** {@code \b}는 JDK 19부터 한글을 단어 문자로 보지 않아 JDK마다 결과가 달랐다. 영문자 기준 lookaround로 고정한다. */
    private static final Pattern KEYWORD =
            Pattern.compile("(?<![A-Za-z])(MUST NOT|SHOULD NOT|MUST|SHOULD)(?![A-Za-z])");
    /** 용어 규칙을 설명하는 줄이라 규범 문장이 아니다. */
    private static final String KEYWORD_DEFINITION = "- MUST, MUST NOT";

    private final Map<String, Integer> keywordSections = new TreeMap<>();

    private RequirementIndex() {}

    static RequirementIndex parse(String markdown) {
        RequirementIndex index = new RequirementIndex();
        String current = null;
        for (String line : markdown.split("\n", -1)) {
            Matcher heading = HEADING.matcher(line);
            if (heading.find()) {
                current = heading.group(1);
            } else if (current != null
                    && !line.startsWith(KEYWORD_DEFINITION)
                    && KEYWORD.matcher(line).find()) {
                index.keywordSections.merge(current, 1, Integer::sum);
            }
        }
        return index;
    }

    /** 규범 키워드가 있는 절 번호와 그 줄 수. */
    Map<String, Integer> keywordSections() {
        return keywordSections;
    }
}
