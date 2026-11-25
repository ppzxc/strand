package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ResultCertaintyTest {

    private static List<JsonObject> resultEvents(JsonObject vector) {
        List<JsonObject> results = new ArrayList<>();
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            for (JsonElement event : step.getAsJsonObject().getAsJsonArray("events")) {
                if ("result".equals(event.getAsJsonObject().get("event").getAsString())) {
                    results.add(event.getAsJsonObject());
                }
            }
        }
        return results;
    }

    @Test
    void everyResultEventInEveryVectorStatesItsCertainty() {
        int seen = 0;
        for (Map.Entry<String, JsonObject> entry : RawVectors.byKey().entrySet()) {
            for (JsonObject result : resultEvents(entry.getValue())) {
                seen++;
                assertThat(result.has("certainty")).as(entry.getKey()).isTrue();
                assertThat(result.get("certainty").getAsString())
                        .as(entry.getKey())
                        .isIn("response", "not_processed", "unknown");
            }
        }
        assertThat(seen).isGreaterThan(40);
    }

    @Test
    void certaintyMatchesTheStatusItAccompanies() {
        for (Map.Entry<String, JsonObject> entry : RawVectors.byKey().entrySet()) {
            for (JsonObject result : resultEvents(entry.getValue())) {
                String status = result.get("status").getAsString();
                String certainty = result.get("certainty").getAsString();
                if ("OK".equals(status)) {
                    assertThat(certainty).as(entry.getKey()).isEqualTo("response");
                }
                if (!result.has("stream_id")) {

                    assertThat(certainty).as(entry.getKey()).isEqualTo("not_processed");
                    assertThat(status).as(entry.getKey()).isIn("UNAVAILABLE", "CANCELLED", "DEADLINE_EXCEEDED");
                }
            }
        }
    }

    @Test
    void requestsFailingBeforeTheStreamOpensAssertANotProcessedResultWithoutStreamId() {
        for (String key : Arrays.asList(
                "close-races/request-after-close-sent-fails-locally",
                "close-races/request-after-close-received-fails-locally-server",
                "close-races/close-received-blocks-opening-new-stream",
                "streams/exhaustion-after-last-number-sends-close-client",
                "streams/exhaustion-after-last-number-sends-close-server",
                "streams/exhaustion-after-close-received-sends-no-close",
                "streams/exhaustion-after-close-sent-sends-no-second-close",
                "streams/exhaustion-with-pending-stream-closes-after-it-ends",
                "streams/exhaustion-takes-precedence-over-peer-limit",
                "backpressure/local-failure-at-peer-limit-sends-nothing-and-keeps-the-number")) {
            JsonObject vector = RawVectors.byKey().get(key);
            assertThat(vector).as(key).isNotNull();
            assertThat(resultEvents(vector))
                    .as(key)
                    .anyMatch(r -> !r.has("stream_id")
                            && "UNAVAILABLE".equals(r.get("status").getAsString())
                            && "not_processed".equals(r.get("certainty").getAsString()));
        }
    }

    private static List<JsonObject> resultsOf(String key) {
        JsonObject vector = RawVectors.byKey().get(key);
        assertThat(vector).as(key).isNotNull();
        List<JsonObject> results = resultEvents(vector);
        assertThat(results).as(key).isNotEmpty();
        return results;
    }

    @Test
    void refusedResetAndConnectionEndAreDistinguished() {
        assertThat(resultsOf("streams/sender-reset-received-refused"))
                .allMatch(r -> "not_processed".equals(r.get("certainty").getAsString()));
        assertThat(resultsOf("close-races/disconnect-fails-pending-request-with-unavailable"))
                .allMatch(r -> "unknown".equals(r.get("certainty").getAsString()));
        assertThat(resultsOf("streams/sender-own-cancel-sent"))
                .allMatch(r -> "unknown".equals(r.get("certainty").getAsString()));
    }

    @Test
    void deadlineVectorsAssertBothOutcomesOfSection441() {
        JsonObject queued = RawVectors.byKey().get("streams/request-deadline-of-queued-request-ends-without-stream-id");
        assertThat(queued).isNotNull();
        assertThat(resultEvents(queued)).singleElement().satisfies(r -> {
            assertThat(r.has("stream_id")).isFalse();
            assertThat(r.get("status").getAsString()).isEqualTo("DEADLINE_EXCEEDED");
            assertThat(r.get("certainty").getAsString()).isEqualTo("not_processed");
        });
        JsonObject written = RawVectors.byKey().get("streams/request-deadline-after-write-sends-reset-cancel");
        assertThat(written).isNotNull();
        assertThat(resultEvents(written)).singleElement().satisfies(r -> {
            assertThat(r.get("stream_id").getAsString()).isEqualTo("1");
            assertThat(r.get("status").getAsString()).isEqualTo("DEADLINE_EXCEEDED");
            assertThat(r.get("certainty").getAsString()).isEqualTo("unknown");
        });
    }

    @Test
    void formatDocumentSection62ListsTheQueuedRequestOutcomes() throws java.io.IOException {
        java.nio.file.Path doc =
                RequirementIndex.repositoryRoot().resolve("docs/specification/STRAND_CONFORMANCE_VECTORS.md");
        String text = new String(java.nio.file.Files.readAllBytes(doc), java.nio.charset.StandardCharsets.UTF_8);
        String section = text.substring(text.indexOf("### 6.2"), text.indexOf("## 7."));
        assertThat(section)
                .contains("`UNAVAILABLE`", "`CANCELLED`", "`DEADLINE_EXCEEDED`")
                .doesNotContain("`status`가 `UNAVAILABLE`이고 `certainty`가 `not_processed`여야 한다");
        assertThat(text.substring(text.indexOf("## 5. 단계"), text.indexOf("## 6.")))
                .contains("`deadline_ms`");
    }
}
