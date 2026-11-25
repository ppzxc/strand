package io.github.ppzxc.strand.conformance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class UncoveredRequirements {

    static final String UNOBSERVABLE = "관측불가";
    static final String UNSUPPORTED_FORMAT = "형식미지원";
    static final List<String> CATEGORIES =
            Collections.unmodifiableList(Arrays.asList(UNOBSERVABLE, UNSUPPORTED_FORMAT));

    private final Map<String, String[]> entries = new LinkedHashMap<>();

    private final List<String> problems = new ArrayList<>();

    private UncoveredRequirements() {}

    static UncoveredRequirements parse(List<String> lines) {
        UncoveredRequirements result = new UncoveredRequirements();
        for (String line : significant(lines)) {
            String[] parts = line.split("\\s+", 3);
            if (parts.length < 3) {
                result.problems.add("`req-… 범주 사유` 형식이 아니다: " + line);
                continue;
            }
            if (!Check.REQUIREMENT_ID.matcher(parts[0]).matches()) {
                result.problems.add("첫 칸이 요구사항 식별자가 아니다: " + line);
                continue;
            }
            if (!CATEGORIES.contains(parts[1])) {
                result.problems.add(parts[0] + "의 범주 표지가 " + CATEGORIES + " 중 하나가 아니다: " + parts[1]);
                continue;
            }
            if (result.entries.containsKey(parts[0])) {
                result.problems.add("식별자가 두 번 올랐다: " + parts[0]);
                continue;
            }
            result.entries.put(parts[0], new String[] {parts[1], parts[2]});
        }
        return result;
    }

    static UncoveredRequirements load() throws IOException {
        return parse(resourceLines("/uncovered-requirements.txt"));
    }

    static List<String> dataLines(String name) throws IOException {
        return significant(resourceLines(name));
    }

    private static List<String> significant(List<String> lines) {
        return lines.stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .collect(Collectors.toList());
    }

    static List<String> resourceLines(String name) throws IOException {
        InputStream stream = UncoveredRequirements.class.getResourceAsStream(name);
        if (stream == null) {
            throw new IllegalStateException(name + " 리소스가 없다");
        }
        try (Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
                BufferedReader lines = new BufferedReader(reader)) {
            return lines.lines().collect(Collectors.toList());
        }
    }

    List<String> problems() {
        return Collections.unmodifiableList(problems);
    }

    Set<String> ids() {
        return Collections.unmodifiableSet(entries.keySet());
    }

    String category(String id) {
        return entries.get(id)[0];
    }

    String reason(String id) {
        return entries.get(id)[1];
    }
}
