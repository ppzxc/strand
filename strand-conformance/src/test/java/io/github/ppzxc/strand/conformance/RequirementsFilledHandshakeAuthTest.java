package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * 이관 중(#282)에 handshake·hello-establishment·auth·websocket 벡터가 모두 {@code requirements}를 갖는지 본다. 모든
 * 벡터에 {@code requirements}를 필수로 만드는 마지막 티켓이 이 검사를 대신한다.
 */
class RequirementsFilledHandshakeAuthTest {

    private static final List<String> FILES = Arrays.asList("handshake", "hello-establishment", "auth", "websocket");

    @Test
    void everyVectorInTheMigratedFilesNamesAtLeastOneRequirement() {
        List<String> missing = new ArrayList<>();
        int checked = 0;
        for (Map.Entry<String, JsonObject> vector : RawVectors.byKey().entrySet()) {
            String file = vector.getKey().substring(0, vector.getKey().indexOf('/'));
            if (!FILES.contains(file)) {
                continue;
            }
            checked++;
            JsonObject value = vector.getValue();
            if (!value.has("requirements")
                    || value.getAsJsonArray("requirements").isEmpty()) {
                missing.add(vector.getKey());
            }
        }
        assertThat(checked).as("대상 벡터를 읽었다").isEqualTo(108);
        assertThat(missing).as("requirements가 없는 벡터").isEmpty();
    }
}
