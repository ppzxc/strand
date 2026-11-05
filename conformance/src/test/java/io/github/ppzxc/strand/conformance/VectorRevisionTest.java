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
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** 규격 0.1.0에 맞춰 개정한 실제 벡터 디렉터리가 지킬 규칙(#97). 벡터 목록 자체는 단언하지 않는다. */
class VectorRevisionTest {

    private static final Path SPEC = VectorSuite.defaultDirectory()
            .resolve("../../docs/specification/STRAND_SPECIFICATION.md")
            .normalize();

    /** 삭제된 개념의 흔적. 벡터 JSON 어디에도 나오면 안 된다. */
    private static final List<String> REMOVED_TERMS = Arrays.asList(
            "last_stream_id",
            "content_type",
            "max_pending_bytes",
            "draining",
            "hello_timeout_ms",
            "\"END\"",
            "\"RESPONSE\"",
            "STREAM_CLOSED",
            "FRAME_SIZE_ERROR",
            "HELLO_TIMEOUT");

    /** 삭제된 개념이 들어간 id 조각. 새 용어로 바꿔야 한다. */
    private static final List<String> REMOVED_ID_WORDS =
            Arrays.asList("frame-size-error", "hello-timeout", "pending-bytes", "pending-return", "last-stream-id");

    private static List<Path> vectorFiles() throws IOException {
        try (Stream<Path> files = Files.list(VectorSuite.defaultDirectory())) {
            return files.filter(p -> p.toString().endsWith(".json")).sorted().collect(Collectors.toList());
        }
    }

    private static List<JsonObject> vectors(Path file) throws IOException {
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            List<JsonObject> result = new ArrayList<>();
            for (JsonElement v :
                    JsonParser.parseReader(reader).getAsJsonObject().getAsJsonArray("vectors")) {
                result.add(v.getAsJsonObject());
            }
            return result;
        }
    }

    private static List<String> allIds() throws IOException {
        List<String> ids = new ArrayList<>();
        for (Path file : vectorFiles()) {
            for (JsonObject vector : vectors(file)) {
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
        for (Path file : vectorFiles()) {
            String text = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
            for (String term : REMOVED_TERMS) {
                assertThat(text).as("%s의 %s", file.getFileName(), term).doesNotContain(term);
            }
        }
    }

    @Test
    void everySpecReferencePointsToAnExistingSection() throws IOException {
        List<String> sections = specSections();
        assertThat(sections).contains("5.1", "3.2.5", "2.2.5").doesNotContain("5.1.1", "5.1.2", "5.1.3");
        for (Path file : vectorFiles()) {
            for (JsonObject vector : vectors(file)) {
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
                vectorFiles().stream().map(p -> p.getFileName().toString()).collect(Collectors.toList());

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
