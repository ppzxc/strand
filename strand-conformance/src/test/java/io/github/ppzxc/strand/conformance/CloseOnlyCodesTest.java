package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** 확인 대기 초과의 CLOSE 사유와 CLOSE 전용 코드를 담은 RESET을 벡터가 단언하는지 본다(#163). */
class CloseOnlyCodesTest {

    /** 시계를 흘려보내는 단계(advance)에서 구현이 CLOSE를 보내는 것은 확인 대기 초과뿐이다. */
    @Test
    void everyCloseSentWhenTheClockAdvancesIsUnresponsive() {
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
                        assertThat(frame.get("error_code").getAsString())
                                .as(entry.getKey())
                                .isEqualTo("UNRESPONSIVE");
                    }
                }
            }
        }
        assertThat(seen).isGreaterThanOrEqualTo(6);
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
        assertThat(v.getAsJsonArray("spec").toString()).contains("2.5.2", "4.4");
    }

    @Test
    void reservedErrorCodeVectorUsesTheFirstReservedValue() {
        JsonObject v = RawVectors.byKey().get("streams/reset-received-with-reserved-error-code-gives-internal");
        String hex =
                v.getAsJsonArray("steps").get(1).getAsJsonObject().get("hex").getAsString();
        assertThat(hex).endsWith("00000008");
    }
}
