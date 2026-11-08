package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** 규격 0.1.0에 맞춰 개정한 실제 벡터 디렉터리가 지킬 규칙(#97). 벡터 목록 자체는 단언하지 않는다. */
class VectorRevisionTest {

    private static final Path SPEC = VectorSuite.defaultDirectory()
            .resolve("../../docs/specification/STRAND_SPECIFICATION.md")
            .normalize();

    /** 삭제된 개념의 JSON 키 이름. 어느 깊이의 객체에도 나오면 안 된다. */
    private static final List<String> REMOVED_KEYS = Arrays.asList(
            "content_type",
            "content_types",
            "max_pending_bytes",
            "draining",
            "draining_retention_limit",
            "hello_timeout_ms");

    /** 값이 열거형·플래그·오류 코드인 키. 자유 서술(description, id)과 달리 이 위치의 문자열만 삭제 이름과 비교한다. */
    private static final List<String> ENUM_KEYS =
            Arrays.asList("type", "kind", "event", "status", "error_code", "flags");

    /** 삭제된 열거형·플래그·오류 코드 값. */
    private static final List<String> REMOVED_ENUM_VALUES =
            Arrays.asList("END", "RESPONSE", "STREAM_CLOSED", "FRAME_SIZE_ERROR", "HELLO_TIMEOUT");

    /** 삭제된 개념이 들어간 id 조각. 새 용어로 바꿔야 한다. */
    private static final List<String> REMOVED_ID_WORDS = Arrays.asList(
            "frame-size-error",
            "hello-timeout",
            "pending-bytes",
            "pending-return",
            "draining",
            "stream-closed",
            "content-type");

    /** JSON 트리에서 삭제된 개념이 나타나는 위치(경로 문자열)를 모은다. */
    private static List<String> removedConceptPaths(JsonElement root) {
        List<String> found = new ArrayList<>();
        collectRemoved(root, "$", false, found);
        return found;
    }

    private static void collectRemoved(JsonElement node, String path, boolean enumPosition, List<String> found) {
        if (node.isJsonObject()) {
            for (java.util.Map.Entry<String, JsonElement> member :
                    node.getAsJsonObject().entrySet()) {
                String child = path + "." + member.getKey();
                if (REMOVED_KEYS.contains(member.getKey())) {
                    found.add(child);
                }
                collectRemoved(member.getValue(), child, ENUM_KEYS.contains(member.getKey()), found);
            }
        } else if (node.isJsonArray()) {
            int i = 0;
            for (JsonElement item : node.getAsJsonArray()) {
                collectRemoved(item, path + "[" + i++ + "]", enumPosition, found);
            }
        } else if (enumPosition
                && node.getAsJsonPrimitive().isString()
                && REMOVED_ENUM_VALUES.contains(node.getAsString())) {
            found.add(path + "=" + node.getAsString());
        }
    }

    private static List<String> allIds() throws IOException {
        List<String> ids = new ArrayList<>();
        for (Path file : RawVectors.files()) {
            for (JsonObject vector : RawVectors.vectors(file)) {
                ids.add(vector.get("id").getAsString());
            }
        }
        return ids;
    }

    private static List<String> deletedIds() throws IOException {
        try (Reader reader = new InputStreamReader(
                VectorRevisionTest.class.getResourceAsStream("/deleted-vector-ids.txt"), StandardCharsets.UTF_8)) {
            return new BufferedReader(reader)
                    .lines()
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .collect(Collectors.toList());
        }
    }

    /** 규격 본문의 제목에 실제로 있는 절 번호. */
    private static List<String> specSections() throws IOException {
        Pattern heading = Pattern.compile("^#{2,4} (\\d+(?:\\.\\d+)*)\\.?\\s");
        List<String> sections = new ArrayList<>();
        for (String line : Files.readAllLines(SPEC, StandardCharsets.UTF_8)) {
            Matcher m = heading.matcher(line);
            if (m.find()) {
                sections.add(m.group(1));
            }
        }
        return sections;
    }

    @Test
    void deletedVectorIdsAreAbsentFromEveryFile() throws IOException {
        assertThat(deletedIds()).hasSize(62);
        assertThat(allIds()).doesNotContainAnyElementsOf(deletedIds());
    }

    @Test
    void removedConceptsDoNotAppearInVectorFiles() throws IOException {
        for (Path file : RawVectors.files()) {
            try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                assertThat(removedConceptPaths(JsonParser.parseReader(reader)))
                        .as("%s", file.getFileName())
                        .isEmpty();
            }
        }
    }

    @Test
    void removedConceptCheckLooksAtKeysAndEnumPositionsOnly() {
        // 자유 서술(description)과 id 안의 일반 단어는 오탐이 아니다.
        assertThat(removedConceptPaths(JsonParser.parseString("{\"id\":\"draining-end\","
                        + "\"description\":\"draining 상태와 END, RESPONSE, HELLO_TIMEOUT를 말로만 언급\","
                        + "\"steps\":[{\"send\":[{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\"}]}]}")))
                .isEmpty();
        // 키 이름
        assertThat(removedConceptPaths(
                        JsonParser.parseString("{\"setup\":{\"local_hello\":{\"max_pending_bytes\":1}}}")))
                .hasSize(1);
        assertThat(removedConceptPaths(JsonParser.parseString("{\"setup\":{\"draining\":true}}")))
                .hasSize(1);
        // enum/flag/error_code 위치의 값
        assertThat(removedConceptPaths(
                        JsonParser.parseString("{\"send\":[{\"type\":\"RESET\",\"error_code\":\"STREAM_CLOSED\"}]}")))
                .hasSize(1);
        assertThat(removedConceptPaths(
                        JsonParser.parseString("{\"send\":[{\"type\":\"CLOSE\",\"error_code\":\"HELLO_TIMEOUT\"}]}")))
                .hasSize(1);
        assertThat(removedConceptPaths(JsonParser.parseString("{\"send\":[{\"type\":256,\"flags\":[\"END\"]}]}")))
                .hasSize(1);
        assertThat(removedConceptPaths(JsonParser.parseString("{\"send\":[{\"type\":\"RESPONSE\"}]}")))
                .hasSize(1);
        assertThat(removedConceptPaths(
                        JsonParser.parseString("{\"send\":[{\"type\":256,\"status\":\"FRAME_SIZE_ERROR\"}]}")))
                .hasSize(1);
    }

    @Test
    void everySpecReferencePointsToAnExistingSection() throws IOException {
        List<String> sections = specSections();
        assertThat(sections).contains("5.1", "3.2.5", "2.2.5").doesNotContain("5.1.1", "5.1.2", "5.1.3");
        for (Path file : RawVectors.files()) {
            for (JsonObject vector : RawVectors.vectors(file)) {
                for (JsonElement ref : vector.getAsJsonArray("spec")) {
                    assertThat(sections)
                            .as(
                                    "%s/%s의 spec %s",
                                    file.getFileName(), vector.get("id").getAsString(), ref.getAsString())
                            .contains(ref.getAsString());
                }
            }
        }
    }

    @Test
    void vectorFilesAreNamedByTheNewNames() throws IOException {
        List<String> names =
                RawVectors.files().stream().map(p -> p.getFileName().toString()).collect(Collectors.toList());

        assertThat(names).contains("backpressure.json", "streams.json");
        assertThat(names).doesNotContain("pending-bytes.json", "stream-boundaries.json");
    }

    @Test
    void idsDoNotUseRemovedConcepts() throws IOException {
        for (String id : allIds()) {
            for (String word : REMOVED_ID_WORDS) {
                assertThat(id).as(id).doesNotContain(word);
            }
        }
    }
}
