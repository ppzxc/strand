package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** 애플리케이션 payload가 불투명 바이트열이라는 규격 구성을 검사한다(#165). Protobuf는 비규범 부록에만 남는다. */
class PayloadOpaqueSpecTest {

    private static final Path SPEC = VectorSuite.defaultDirectory()
            .resolve("../../docs/specification/STRAND_SPECIFICATION.md")
            .normalize();

    private static final String APPENDIX_HEADING = "## 부록 A. Protobuf 매핑 (비규범)";
    private static final Pattern PROTOBUF =
            Pattern.compile("Protobuf|Protocol Buffers|\\.proto|PROTOBUF", Pattern.CASE_INSENSITIVE);
    private static final Pattern FOOTNOTE_DEFINITION = Pattern.compile("^\\[\\^([A-Za-z0-9-]+)\\]:");

    private static List<String> lines() throws IOException {
        return Files.readAllLines(SPEC, StandardCharsets.UTF_8);
    }

    @Test
    void protobufMappingAppendixIsMarkedNonNormative() throws IOException {
        List<String> lines = lines();
        int at = lines.indexOf(APPENDIX_HEADING);
        assertThat(at).as("비규범 부록 제목").isGreaterThanOrEqualTo(0);
        assertThat(lines.subList(at + 1, Math.min(lines.size(), at + 6)))
                .as("부록이 비규범임을 본문에서도 밝힌다")
                .anyMatch(l -> l.contains("비규범"));
    }

    @Test
    void normativeBodyDoesNotMentionProtobuf() throws IOException {
        List<String> lines = lines();
        int appendix = lines.indexOf(APPENDIX_HEADING);
        assertThat(appendix).isGreaterThanOrEqualTo(0);
        List<String> offending = new ArrayList<>();
        for (int i = 0; i < appendix; i++) {
            if (!FOOTNOTE_DEFINITION.matcher(lines.get(i)).find()
                    && PROTOBUF.matcher(lines.get(i)).find()) {
                offending.add((i + 1) + ": " + lines.get(i));
            }
        }
        assertThat(offending).as("규범 본문은 payload 형식을 요구하지 않는다").isEmpty();
    }

    @Test
    void everyFootnoteDefinitionIsCited() throws IOException {
        List<String> lines = lines();
        List<String> uncited = new ArrayList<>();
        for (String line : lines) {
            Matcher m = FOOTNOTE_DEFINITION.matcher(line);
            if (!m.find()) {
                continue;
            }
            String ref = "[^" + m.group(1) + "]";
            boolean cited =
                    lines.stream().anyMatch(l -> !FOOTNOTE_DEFINITION.matcher(l).find() && l.contains(ref));
            if (!cited) {
                uncited.add(ref);
            }
        }
        assertThat(uncited).as("인용하지 않는 각주").isEmpty();
    }
}
