package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RawVectorsTest {

    private static JsonObject vector(String id) {
        return JsonParser.parseString("{\"id\":\"" + id + "\"}").getAsJsonObject();
    }

    @Test
    void aDuplicateKeyIsRejected() {
        Map<String, JsonObject> target = new LinkedHashMap<>();
        RawVectors.put(target, "f/a", vector("a"));

        assertThatThrownBy(() -> RawVectors.put(target, "f/a", vector("a")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("f/a");
        assertThat(target).hasSize(1);
    }

    @Test
    void filesAreReadOncePerVectorDirectory() {
        assertThat(RawVectors.files()).isSameAs(RawVectors.files());
    }

    @Test
    void vectorsOfAFileAreReadOnce() {
        assertThat(RawVectors.files()).isNotEmpty();
        Path file = RawVectors.files().get(0);

        assertThat(RawVectors.vectors(file)).isSameAs(RawVectors.vectors(file));
    }

    @Test
    void aChangedVectorDirectoryIsReadAgain(@TempDir Path other) throws IOException {
        Files.write(other.resolve("only.json"), "{\"vectors\":[{\"id\":\"x\"}]}".getBytes(StandardCharsets.UTF_8));
        String original = System.getProperty(VectorSuite.VECTORS_DIR_PROPERTY);
        List<Path> before = RawVectors.files();
        try {
            System.setProperty(VectorSuite.VECTORS_DIR_PROPERTY, other.toString());

            assertThat(RawVectors.files()).isNotEqualTo(before).hasSize(1);
            assertThat(RawVectors.byKey()).containsOnlyKeys("only/x");
        } finally {
            System.setProperty(VectorSuite.VECTORS_DIR_PROPERTY, original);
        }
        assertThat(RawVectors.files()).isEqualTo(before);
    }
}
