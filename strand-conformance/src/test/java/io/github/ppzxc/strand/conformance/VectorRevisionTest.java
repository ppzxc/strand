package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class VectorRevisionTest {

    private static final Path SPEC = RequirementIndex.specificationFile();

    private static final List<String> REMOVED_KEYS = Arrays.asList(
            "content_type",
            "content_types",
            "max_pending_bytes",
            "draining",
            "draining_retention_limit",
            "hello_timeout_ms");

    private static final List<String> ENUM_KEYS =
            Arrays.asList("type", "kind", "event", "status", "error_code", "flags");

    private static final List<String> REMOVED_ENUM_VALUES =
            Arrays.asList("END", "RESPONSE", "STREAM_CLOSED", "FRAME_SIZE_ERROR", "HELLO_TIMEOUT", "OVERLOAD");

    private static final List<String> REMOVED_ID_WORDS = Arrays.asList(
            "frame-size-error",
            "hello-timeout",
            "pending-bytes",
            "pending-return",
            "draining",
            "stream-closed",
            "content-type");

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
        return UncoveredRequirements.dataLines("/deleted-vector-ids.txt");
    }

    @Test
    void deletedVectorIdsAreAbsentFromEveryFile() throws IOException {
        assertThat(deletedIds()).hasSize(67);
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

        assertThat(removedConceptPaths(JsonParser.parseString("{\"id\":\"draining-end\","
                        + "\"description\":\"draining 상태와 END, RESPONSE, HELLO_TIMEOUT를 말로만 언급\","
                        + "\"steps\":[{\"send\":[{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\"}]}]}")))
                .isEmpty();

        assertThat(removedConceptPaths(
                        JsonParser.parseString("{\"setup\":{\"local_hello\":{\"max_pending_bytes\":1}}}")))
                .hasSize(1);
        assertThat(removedConceptPaths(JsonParser.parseString("{\"setup\":{\"draining\":true}}")))
                .hasSize(1);

        assertThat(removedConceptPaths(
                        JsonParser.parseString("{\"send\":[{\"type\":\"RESET\",\"error_code\":\"STREAM_CLOSED\"}]}")))
                .hasSize(1);
        assertThat(removedConceptPaths(
                        JsonParser.parseString("{\"send\":[{\"type\":\"CLOSE\",\"error_code\":\"HELLO_TIMEOUT\"}]}")))
                .hasSize(1);
        assertThat(removedConceptPaths(
                        JsonParser.parseString("{\"send\":[{\"type\":\"CLOSE\",\"error_code\":\"OVERLOAD\"}]}")))
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
    void everyRequirementReferencePointsToAnExistingIdentifier() {
        RequirementIndex index = RequirementIndex.ofSpecification();
        assertThat(index.requirement("req-check-new-request-order")).isNotNull();
        for (Path file : RawVectors.files()) {
            for (JsonObject vector : RawVectors.vectors(file)) {
                for (String id : RawVectors.requirementsOf(vector)) {
                    assertThat(index.requirement(id))
                            .as(
                                    "%s/%s의 requirements %s",
                                    file.getFileName(), vector.get("id").getAsString(), id)
                            .isNotNull();
                }
            }
        }
    }

    @Test
    void specificationDoesNotUseTheRemovedErrorCodeName() throws IOException {
        assertThat(String.join("\n", Files.readAllLines(SPEC, StandardCharsets.UTF_8)))
                .doesNotContain("OVERLOAD");
    }

    @Test
    void vectorFilesDoNotMentionTheRemovedErrorCodeName() throws IOException {
        for (Path file : RawVectors.files()) {
            assertThat(String.join("\n", Files.readAllLines(file, StandardCharsets.UTF_8)))
                    .as("%s", file.getFileName())
                    .doesNotContain("OVERLOAD");
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
