package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** streams, request-checks, extensions, backpressure 벡터가 모두 단언하는 요구사항을 적었는지 본다(#283). */
class StreamsRequirementsFilledTest {

    private static final List<String> MIGRATED =
            Arrays.asList("streams", "request-checks", "extensions", "backpressure");

    @Test
    void everyVectorInTheMigratedFilesNamesItsRequirements() {
        List<String> missing = new ArrayList<>();
        int seen = 0;
        for (Map.Entry<String, JsonObject> entry : RawVectors.byKey().entrySet()) {
            String file = entry.getKey().substring(0, entry.getKey().indexOf('/'));
            if (!MIGRATED.contains(file)) {
                continue;
            }
            seen++;
            if (!entry.getValue().has("requirements")
                    || entry.getValue().getAsJsonArray("requirements").isEmpty()) {
                missing.add(entry.getKey());
            }
        }
        assertThat(seen).as("대상 파일의 벡터 수").isEqualTo(84);
        assertThat(missing).as("requirements가 없는 벡터").isEmpty();
    }
}
