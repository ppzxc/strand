package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

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
}
