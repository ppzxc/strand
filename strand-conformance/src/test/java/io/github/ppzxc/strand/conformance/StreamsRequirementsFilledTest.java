package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/** streams, request-checks, extensions, backpressure 벡터가 모두 단언하는 요구사항을 적었는지 본다(#283). */
class StreamsRequirementsFilledTest {

    private static final List<String> MIGRATED =
            Arrays.asList("streams", "request-checks", "extensions", "backpressure");

    @Test
    void everyVectorInTheMigratedFilesNamesItsRequirements() {
        List<String> missing = new ArrayList<>();
        Set<String> filesSeen = new TreeSet<>();
        for (Map.Entry<String, JsonObject> entry : RawVectors.byKey().entrySet()) {
            String key = entry.getKey();
            String file = key.substring(0, Math.max(0, key.indexOf('/')));
            if (!MIGRATED.contains(file)) {
                continue;
            }
            filesSeen.add(file);
            if (!entry.getValue().has("requirements")
                    || entry.getValue().getAsJsonArray("requirements").isEmpty()) {
                missing.add(entry.getKey());
            }
        }
        assertThat(filesSeen).as("벡터를 읽은 대상 파일").containsExactlyInAnyOrderElementsOf(MIGRATED);
        assertThat(missing).as("requirements가 없는 벡터").isEmpty();
    }
}
