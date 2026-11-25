package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CloseOnlyCodesTest {

    @Test
    void everyCloseSentWhenTheClockAdvancesIsUnresponsiveOrTheAuthWaitTimeout() {
        int seen = 0;
        for (Map.Entry<String, JsonObject> entry : RawVectors.byKey().entrySet()) {
            for (JsonElement step : entry.getValue().getAsJsonArray("steps")) {
                JsonObject s = step.getAsJsonObject();
                if (!"advance".equals(s.get("kind").getAsString())) {
                    continue;
                }
                for (JsonElement f : s.getAsJsonArray("send")) {
                    JsonObject frame = f.getAsJsonObject();
                    if ("CLOSE".equals(frame.get("type").getAsString())) {
                        seen++;
                        String expected = declaresAuthTimeout(entry.getValue())
                                ? "UNAUTHENTICATED"
                                : declaresCredentialExpiry(entry.getValue()) ? "NO_ERROR" : "UNRESPONSIVE";
                        assertThat(frame.get("error_code").getAsString())
                                .as(entry.getKey())
                                .isEqualTo(expected);
                    }
                }
            }
        }
        assertThat(seen).isGreaterThanOrEqualTo(6);
    }

    private static boolean declaresAuthTimeout(JsonObject vector) {
        JsonObject setup = vector.getAsJsonObject("setup");
        return setup.has("config") && setup.getAsJsonObject("config").has("auth_timeout_ms");
    }

    private static boolean declaresCredentialExpiry(JsonObject vector) {
        JsonObject setup = vector.getAsJsonObject("setup");
        return setup.has("auth") && setup.getAsJsonObject("auth").has("lifetime_seconds");
    }

    @Test
    void aVectorReceivesResetWithEachCloseOnlyCodeAndGivesInternal() {
        JsonObject v = RawVectors.byKey().get("streams/reset-received-with-close-only-error-codes-gives-internal");
        assertThat(v).isNotNull();
        List<String> hexes = new ArrayList<>();
        int internal = 0;
        for (JsonElement step : v.getAsJsonArray("steps")) {
            JsonObject s = step.getAsJsonObject();
            if ("recv".equals(s.get("kind").getAsString())) {
                String hex = s.get("hex").getAsString();
                hexes.add(hex.substring(hex.length() - 8));
            }
            for (JsonElement e : s.getAsJsonArray("events")) {
                JsonObject event = e.getAsJsonObject();
                if ("result".equals(event.get("event").getAsString())) {
                    assertThat(event.get("status").getAsString()).isEqualTo("INTERNAL");
                    assertThat(event.get("certainty").getAsString()).isEqualTo("unknown");
                    internal++;
                }
            }
        }
        assertThat(hexes).containsExactly("00000005", "00000006", "00000007");
        assertThat(internal).isEqualTo(3);

        assertThat(sectionsOf("streams/reset-received-with-close-only-error-codes-gives-internal", v))
                .contains("4.4");
    }

    private static List<String> sectionsOf(String key, JsonObject vector) {
        return RequirementIndex.ofSpecification().sectionsOf(key, RawVectors.requirementsOf(vector));
    }

    @Test
    void reservedErrorCodeVectorUsesTheFirstReservedValue() {
        JsonObject v = RawVectors.byKey().get("streams/reset-received-with-reserved-error-code-gives-internal");
        String hex =
                v.getAsJsonArray("steps").get(1).getAsJsonObject().get("hex").getAsString();
        assertThat(hex).endsWith("00000008");

        assertThat(sectionsOf("streams/reset-received-with-reserved-error-code-gives-internal", v))
                .contains("2.5.2");
    }
}
