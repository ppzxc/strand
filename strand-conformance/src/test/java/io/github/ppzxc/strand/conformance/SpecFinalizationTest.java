package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** 규격 0.2.0 마무리 검수(#167): 용어집과 1.3절의 대응, 형식 문서 예시의 형식, 남은 0.1.0 흔적. core를 실행하지 않는다. */
class SpecFinalizationTest {

    private static final Path ROOT =
            VectorSuite.defaultDirectory().resolve("../..").normalize();
    private static final Path SPEC = ROOT.resolve("docs/specification/STRAND_SPECIFICATION.md");
    private static final Path FORMAT = ROOT.resolve("docs/specification/STRAND_CONFORMANCE_VECTORS.md");
    private static final Path GLOSSARY = ROOT.resolve("GLOSSARY.md");

    private static List<String> lines(Path file) throws IOException {
        return Files.readAllLines(file, StandardCharsets.UTF_8);
    }

    private static String read(Path file) throws IOException {
        return String.join("\n", lines(file));
    }

    /** 규격 1.3절 표의 첫 열에서 굵게 쓴 용어 이름(예: {@code 연결 (Connection)}). */
    private static Set<String> specTerms() throws IOException {
        Set<String> terms = new LinkedHashSet<>();
        boolean in = false;
        for (String line : lines(SPEC)) {
            if (line.startsWith("### ")) {
                in = line.startsWith("### 1.3 ");
            } else if (in && line.startsWith("| **")) {
                Matcher m = Pattern.compile("^\\| \\*\\*(.+?)\\*\\*").matcher(line);
                if (m.find()) {
                    terms.add(m.group(1).replaceAll("\\*\\*.*", ""));
                }
            }
        }
        return terms;
    }

    @Test
    void glossaryHasNoPendingSpecMarkers() throws IOException {
        assertThat(read(GLOSSARY)).doesNotContain("규격에 추가 예정").doesNotContain("개정 예정");
    }

    @Test
    void everyGlossaryTermLinkedToSection13IsDefinedThere() throws IOException {
        List<String> glossary = lines(GLOSSARY);
        Set<String> terms = specTerms();
        List<String> missing = new ArrayList<>();
        for (int i = 0; i + 1 < glossary.size(); i++) {
            Matcher name = Pattern.compile("^\\*\\*(.+?)\\*\\*:$").matcher(glossary.get(i));
            if (name.find() && glossary.get(i + 1).contains("STRAND_SPECIFICATION.md#13-용어")) {
                if (!terms.contains(name.group(1))) {
                    missing.add(name.group(1));
                }
            }
        }
        assertThat(missing).as("용어집이 1.3절 정의로 링크하지만 1.3절 표에 없는 용어").isEmpty();
    }

    @Test
    void headOfLineBlockingIsDefinedInSection13() throws IOException {
        assertThat(specTerms()).contains("줄 앞 막힘 (Head-of-Line Blocking)");
    }

    @Test
    void glossaryIntroFollowsPositioning() throws IOException {
        String intro = lines(GLOSSARY).stream()
                .filter(l -> l.startsWith("strand는 "))
                .findFirst()
                .orElseThrow(AssertionError::new);
        assertThat(intro).doesNotContain("고성능").contains("단일 연결");
    }

    @Test
    void timeoutHintIsDistinguishedFromRequestDeadline() throws IOException {
        String row = lines(SPEC).stream()
                .filter(l -> l.startsWith("| `0x0001` | `timeout`"))
                .findFirst()
                .orElseThrow(AssertionError::new);
        assertThat(row).as("2.4.2절 timeout은 요청 기한(4.4.1절)과 다른 힌트임을 밝힌다").contains("4.4.1절");
    }

    @Test
    void specificationsLeaveNoVersion010Traces() throws IOException {
        for (Path file : java.util.Arrays.asList(SPEC, FORMAT)) {
            String text = read(file);
            assertThat(text).as(file.getFileName().toString()).doesNotContain("세션");
            assertThat(text).doesNotContain("16바이트").doesNotContain("OVERLOAD").doesNotContain("vector-format-1");
        }
    }

    private static List<String> exampleBlocks() throws IOException {
        List<String> blocks = new ArrayList<>();
        boolean section10 = false;
        boolean inBlock = false;
        StringBuilder current = new StringBuilder();
        for (String line : lines(FORMAT)) {
            if (line.startsWith("## ")) {
                section10 = line.startsWith("## 10. ");
            } else if (section10 && line.equals("```json")) {
                inBlock = true;
                current.setLength(0);
            } else if (inBlock && line.equals("```")) {
                inBlock = false;
                blocks.add(current.toString());
            } else if (inBlock) {
                current.append(line).append('\n');
            }
        }
        return blocks;
    }

    @Test
    void examplesAreFormat2VectorsThatLoad() throws IOException {
        List<String> blocks = exampleBlocks();
        assertThat(blocks).as("10.1~10.4 예시").hasSizeGreaterThanOrEqualTo(4);
        for (String block : blocks) {
            JsonObject file = JsonParser.parseString(block).getAsJsonObject();
            assertThat(file.get("format").getAsInt()).isEqualTo(2);
            assertThat(VectorLoader.parse("example.json", block)).isNotEmpty();
        }
    }

    @Test
    void exampleHexFramesHave20ByteHeadersAndConsistentLength() throws IOException {
        int frames = 0;
        for (String block : exampleBlocks()) {
            for (JsonElement vector :
                    JsonParser.parseString(block).getAsJsonObject().getAsJsonArray("vectors")) {
                for (JsonElement step : vector.getAsJsonObject().getAsJsonArray("steps")) {
                    JsonObject s = step.getAsJsonObject();
                    if (!s.has("hex")) {
                        continue;
                    }
                    byte[] bytes = Bytes.fromHex(s.get("hex").getAsString());
                    int at = 0;
                    if (bytes.length >= 8
                            && Bytes.toHex(java.util.Arrays.copyOf(bytes, 6)).equals("c0de53545230")) {
                        at = 8;
                    }
                    while (at < bytes.length) {
                        assertThat(bytes.length - at).as("헤더 20바이트 이상: %s", s).isGreaterThanOrEqualTo(20);
                        long length = Bytes.readUnsignedInt(bytes, at);
                        assertThat(length).as("length는 16 이상: %s", s).isGreaterThanOrEqualTo(16);
                        assertThat(at + 4L + length)
                                .as("프레임 경계가 hex 끝과 맞는다: %s", s)
                                .isLessThanOrEqualTo(bytes.length);
                        at = (int) (at + 4L + length);
                        frames++;
                    }
                    assertThat(at).as("남는 바이트가 없다: %s", s).isEqualTo(bytes.length);
                }
            }
        }
        assertThat(frames).isGreaterThanOrEqualTo(8);
    }

    @Test
    void exampleResultsCarryCertaintyAndCloseCarriesLastStreamId() throws IOException {
        int results = 0;
        int closes = 0;
        for (String block : exampleBlocks()) {
            for (JsonElement vector :
                    JsonParser.parseString(block).getAsJsonObject().getAsJsonArray("vectors")) {
                for (JsonElement step : vector.getAsJsonObject().getAsJsonArray("steps")) {
                    JsonObject s = step.getAsJsonObject();
                    for (JsonElement ev : array(s, "events")) {
                        if (ev.getAsJsonObject().get("event").getAsString().equals("result")) {
                            results++;
                            assertThat(ev.getAsJsonObject().has("certainty"))
                                    .as("%s", ev)
                                    .isTrue();
                        }
                    }
                    for (JsonElement frame : array(s, "send")) {
                        if (frame.getAsJsonObject().get("type").getAsString().equals("CLOSE")) {
                            closes++;
                            assertThat(frame.getAsJsonObject().has("last_stream_id"))
                                    .as("%s", frame)
                                    .isTrue();
                        }
                    }
                }
            }
        }
        assertThat(results).isGreaterThanOrEqualTo(1);
        assertThat(closes).isGreaterThanOrEqualTo(1);
    }

    private static JsonArray array(JsonObject o, String key) {
        return o.has(key) ? o.getAsJsonArray(key) : new JsonArray();
    }

    @Test
    void examplesWriteHexInFull() throws IOException {
        assertThat(exampleBlocks())
                .as("형식 문서가 정의하지 않는 반복 표기를 예시가 쓰지 않는다")
                .noneMatch(b -> b.matches("(?s).*<[0-9a-fA-F]{2}x\\d+>.*"));
    }
}
