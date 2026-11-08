package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/** CLOSE의 {@code last_stream_id}와 CLOSE 뒤 새 요청 폐기를 벡터가 빠짐없이 다루는지 본다(#162). */
class CloseLastStreamIdTest {

    private static final Path SPEC = VectorSuite.defaultDirectory()
            .resolve("../../docs/specification/STRAND_SPECIFICATION.md")
            .normalize();

    /** 받는 CLOSE의 payload가 12바이트 미만인 것이 벡터의 목적인 벡터. */
    private static final Set<String> SHORT_CLOSE_VECTORS = new TreeSet<>(Arrays.asList(
            "close-races/close-payload-three-bytes-closes-silently",
            "close-races/close-payload-empty-closes-silently",
            "close-races/malformed-close-fails-pending-request-with-unavailable",
            "close-races/close-payload-eleven-bytes-closes-silently"));

    private static List<JsonObject> steps(JsonObject vector) {
        List<JsonObject> result = new ArrayList<>();
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            result.add(step.getAsJsonObject());
        }
        return result;
    }

    private static List<String> receivedHex(JsonObject step) {
        String kind = step.get("kind").getAsString();
        if ("recv".equals(kind) || "recv_message".equals(kind)) {
            return Arrays.asList(step.get("hex").getAsString().replaceAll("\\s", ""));
        }
        return new ArrayList<>();
    }

    /** hex에서 완전한 프레임만 풀어, CLOSE 프레임마다 payload 바이트 수를 돌려준다. */
    private static List<Long> closeFrames(String hex) {
        List<Long> result = new ArrayList<>();
        int pos = 0;
        while (hex.length() - pos >= 40) {
            long length = Long.parseLong(hex.substring(pos, pos + 8), 16);
            int type = Integer.parseInt(hex.substring(pos + 8, pos + 12), 16);
            long total = (4 + length) * 2;
            if (length < 16 || hex.length() - pos < total) {
                break;
            }
            long extensions = Long.parseLong(hex.substring(pos + 32, pos + 36), 16);
            if (type == 3) {
                result.add(length - 16 - extensions);
            }
            pos += (int) total;
        }
        return result;
    }

    @Test
    void everyCloseTheImplementationSendsComparesLastStreamId() {
        int seen = 0;
        for (Map.Entry<String, JsonObject> entry : RawVectors.byKey().entrySet()) {
            for (JsonObject step : steps(entry.getValue())) {
                for (JsonElement item : step.getAsJsonArray("send")) {
                    JsonObject frame = item.getAsJsonObject();
                    if ("CLOSE".equals(frame.get("type").getAsString())) {
                        seen++;
                        assertThat(frame.has("last_stream_id"))
                                .as(entry.getKey())
                                .isTrue();
                        assertThat(frame.get("last_stream_id").getAsString())
                                .as(entry.getKey())
                                .matches("0|[1-9][0-9]*");
                    }
                }
            }
        }
        assertThat(seen).isGreaterThan(40);
    }

    @Test
    void everyCloseTheImplementationReceivesCarriesLastStreamIdBytes() {
        int seen = 0;
        for (Map.Entry<String, JsonObject> entry : RawVectors.byKey().entrySet()) {
            for (JsonObject step : steps(entry.getValue())) {
                for (String hex : receivedHex(step)) {
                    for (long close : closeFrames(hex)) {
                        seen++;
                        if (!SHORT_CLOSE_VECTORS.contains(entry.getKey())) {
                            assertThat(close).as(entry.getKey()).isGreaterThanOrEqualTo(12);
                        }
                    }
                }
            }
        }
        assertThat(seen).isGreaterThan(20);
    }

    @Test
    void shortCloseVectorsReallyReceiveAClosePayloadBelowTwelveBytes() {
        for (String key : SHORT_CLOSE_VECTORS) {
            JsonObject vector = RawVectors.byKey().get(key);
            assertThat(vector).as(key).isNotNull();
            boolean found = false;
            for (JsonObject step : steps(vector)) {
                for (String hex : receivedHex(step)) {
                    for (long close : closeFrames(hex)) {
                        found = true;
                        assertThat(close).as(key).isLessThan(12);
                    }
                }
            }
            assertThat(found).as(key).isTrue();
        }
    }

    private static final List<String> NEW_VECTORS = Arrays.asList(
            // 엇갈린 요청
            "close-races/close-last-stream-id-ends-crossed-request-as-not-processed",
            "close-races/error-close-last-stream-id-ends-crossed-request-as-not-processed",
            // 잘못된 값
            "close-races/close-last-stream-id-in-own-number-space-is-malformed",
            "close-races/close-last-stream-id-above-highest-sent-is-malformed",
            "close-races/close-payload-eleven-bytes-closes-silently",
            // 처음 CLOSE만 사용
            "close-races/first-close-last-stream-id-is-the-only-one-used",
            // CLOSE 뒤 새 요청 폐기
            "backpressure/request-after-close-received-is-dropped-silently",
            "backpressure/request-after-close-sent-is-dropped-silently",
            "backpressure/request-after-close-with-regressed-number-closes-silently");

    @Test
    void newVectorsExist() {
        for (String key : NEW_VECTORS) {
            assertThat(RawVectors.byKey()).as(key).containsKey(key);
        }
    }

    private static String results(String key) {
        StringBuilder sb = new StringBuilder();
        for (JsonObject step : steps(RawVectors.byKey().get(key))) {
            for (JsonElement e : step.getAsJsonArray("events")) {
                JsonObject event = e.getAsJsonObject();
                if ("result".equals(event.get("event").getAsString())) {
                    sb.append(event.get("stream_id").getAsString())
                            .append('=')
                            .append(event.get("certainty").getAsString())
                            .append(' ');
                }
            }
        }
        return sb.toString().trim();
    }

    @Test
    void crossedRequestsAreNotProcessedAndTheOthersStayUnknown() {
        assertThat(results("close-races/close-last-stream-id-ends-crossed-request-as-not-processed"))
                .startsWith("3=not_processed");
        assertThat(results("close-races/error-close-last-stream-id-ends-crossed-request-as-not-processed"))
                .isEqualTo("3=not_processed 1=unknown");
        assertThat(results("close-races/close-last-stream-id-above-highest-sent-is-malformed"))
                .isEqualTo("1=unknown");
        assertThat(results("close-races/close-last-stream-id-in-own-number-space-is-malformed"))
                .isEqualTo("1=unknown");
        assertThat(results("close-races/first-close-last-stream-id-is-the-only-one-used"))
                .isEqualTo("5=not_processed");
    }

    @Test
    void requestsAfterCloseAreDroppedWithoutAnyOutput() {
        for (String key : Arrays.asList(
                "backpressure/request-after-close-received-is-dropped-silently",
                "backpressure/request-after-close-sent-is-dropped-silently")) {
            JsonObject last = steps(RawVectors.byKey().get(key))
                    .get(steps(RawVectors.byKey().get(key)).size() - 1);
            assertThat(last.get("kind").getAsString()).as(key).isEqualTo("recv");
            assertThat(last.getAsJsonArray("send")).as(key).isEmpty();
            assertThat(last.getAsJsonArray("events")).as(key).isEmpty();
        }
    }

    @Test
    void oldRefusalAfterCloseRuleIsGoneFromSpecification() throws IOException {
        String spec = new String(Files.readAllBytes(SPEC), StandardCharsets.UTF_8);
        assertThat(spec).doesNotContain("보내기로 한 `RESET(REFUSED)`");
        assertThat(spec).doesNotContain("CLOSE를 보냈거나 받은 뒤이거나 `n + 1");
    }

    @Test
    void noVectorRefusesARequestWithResetAfterAnyCloseWasSentOrReceived() {
        for (Map.Entry<String, JsonObject> entry : RawVectors.byKey().entrySet()) {
            boolean closeSeen = false;
            for (JsonObject step : steps(entry.getValue())) {
                for (JsonElement item : step.getAsJsonArray("send")) {
                    JsonObject frame = item.getAsJsonObject();
                    if (closeSeen
                            && "RESET".equals(frame.get("type").getAsString())
                            && "REFUSED".equals(frame.get("error_code").getAsString())) {
                        throw new AssertionError(entry.getKey() + ": CLOSE 뒤의 새 요청은 RESET(REFUSED)가 아니라 조용히 버려진다");
                    }
                }
                for (JsonElement item : step.getAsJsonArray("send")) {
                    if ("CLOSE".equals(item.getAsJsonObject().get("type").getAsString())) {
                        closeSeen = true;
                    }
                }
                for (String hex : receivedHex(step)) {
                    if (!closeFrames(hex).isEmpty()) {
                        closeSeen = true;
                    }
                }
            }
        }
    }

    @Test
    void specificationDefinesLastStreamIdOnce() throws IOException {
        String spec = new String(Files.readAllBytes(SPEC), StandardCharsets.UTF_8);
        assertThat(spec).contains("`last_stream_id` u64").contains("## 3.5").contains("#### 3.5.1");
    }
}
