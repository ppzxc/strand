package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

class NewVectorCoverageTest {

    private static final List<Integer> EXCLUDED = Arrays.asList(28, 32);

    private static Map<Integer, List<String>> required() throws IOException {
        Map<Integer, List<String>> result = new LinkedHashMap<>();
        for (String line : UncoveredRequirements.dataLines("/required-vectors.txt")) {
            String[] parts = line.split(" ");
            int n = Integer.parseInt(parts[0].substring("B-".length()));
            result.computeIfAbsent(n, k -> new ArrayList<>()).add(parts[1]);
        }
        return result;
    }

    private static JsonObject vector(String key) {
        JsonObject vector = RawVectors.byKey().get(key);
        assertThat(vector).as(key).isNotNull();
        return vector;
    }

    @Test
    void everyBItemExceptTheExcludedOnesHasVectors() throws IOException {
        TreeSet<Integer> expected = new TreeSet<>();
        for (int n = 1; n <= 32; n++) {
            if (!EXCLUDED.contains(n)) {
                expected.add(n);
            }
        }
        assertThat(required().keySet()).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void everyRequiredVectorExists() throws IOException {
        Map<String, JsonObject> vectors = RawVectors.byKey();
        for (Map.Entry<Integer, List<String>> entry : required().entrySet()) {
            for (String key : entry.getValue()) {
                assertThat(vectors).as("B-%d의 %s", entry.getKey(), key).containsKey(key);
            }
        }
    }

    @Test
    void aVectorUsesKnownTypesAndExpectsUnimplementedWithoutClosing() {
        JsonObject v = vector("request-checks/unknown-type-request-is-answered-unimplemented");
        assertThat(v.getAsJsonObject("setup").has("known_types")).isTrue();

        assertThat(sentFrames(v)).contains("258/UNIMPLEMENTED").noneMatch(f -> f.startsWith("CLOSE/"));
        assertThat(eventNames(v)).doesNotContain("closed").contains("request_received");
    }

    private static List<String> sentFrames(JsonObject vector) {
        List<String> frames = new ArrayList<>();
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            for (JsonElement f : step.getAsJsonObject().getAsJsonArray("send")) {
                JsonObject frame = f.getAsJsonObject();
                String detail = frame.has("status")
                        ? frame.get("status").getAsString()
                        : frame.has("error_code") ? frame.get("error_code").getAsString() : "";
                frames.add(frame.get("type").getAsString() + "/" + detail);
            }
        }
        return frames;
    }

    private static List<String> eventNames(JsonObject vector) {
        List<String> names = new ArrayList<>();
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            for (JsonElement e : step.getAsJsonObject().getAsJsonArray("events")) {
                names.add(e.getAsJsonObject().get("event").getAsString());
            }
        }
        return names;
    }

    @Test
    void aVectorUsesTheWebSocketTransport() {
        JsonObject setup = vector("websocket/established-ping-is-acknowledged").getAsJsonObject("setup");
        assertThat(setup.get("transport").getAsString()).isEqualTo("websocket");
        List<String> kinds = new ArrayList<>();
        for (JsonElement step :
                vector("websocket/established-ping-is-acknowledged").getAsJsonArray("steps")) {
            kinds.add(step.getAsJsonObject().get("kind").getAsString());
        }
        assertThat(kinds).contains("recv_message").doesNotContain("recv");
    }

    @Test
    void aVectorUsesNextStreamIdForNumberExhaustion() {
        JsonObject client = vector("streams/exhaustion-after-last-number-sends-close-client")
                .getAsJsonObject("setup");
        assertThat(client.get("next_stream_id").getAsString()).isEqualTo("9223372036854775807");
        JsonObject server = vector("streams/exhaustion-after-last-number-sends-close-server")
                .getAsJsonObject("setup");
        assertThat(server.get("next_stream_id").getAsString()).isEqualTo("9223372036854775806");
    }

    @Test
    void aVectorUsesPingStepWithAckTimeout() {
        JsonObject v = vector("ack-timeout/ping-ack-timeout-before-and-after-limit");
        assertThat(v.getAsJsonObject("setup").getAsJsonObject("config").has("ack_timeout_ms"))
                .isTrue();
        List<String> kinds = new ArrayList<>();
        for (JsonElement step : v.getAsJsonArray("steps")) {
            kinds.add(step.getAsJsonObject().get("kind").getAsString());
        }
        assertThat(kinds).contains("ping", "advance");
    }

    @Test
    void timeVectorsNeverReachTheLimitExactly() {
        int declared = 0;
        for (Map.Entry<String, JsonObject> entry : RawVectors.byKey().entrySet()) {
            JsonObject vector = entry.getValue();
            JsonObject setup = vector.getAsJsonObject("setup");
            boolean declaresAckTimeout =
                    (setup.has("config") && setup.getAsJsonObject("config").has("ack_timeout_ms"))
                            || entry.getKey().contains("ack-timeout");
            if (declaresAckTimeout) {
                declared++;
                assertThat(AckWaits.startsAnyWait(vector))
                        .as("%s: 확인 대기 시작(HELLO/PING 송신)을 해석할 수 없다", entry.getKey())
                        .isTrue();
                assertThat(AckWaits.hasAdvance(vector))
                        .as("%s: 확인 대기 한도를 보려면 advance 단계가 있어야 한다", entry.getKey())
                        .isTrue();
            }
            boolean declaresAuthTimeout =
                    (setup.has("config") && setup.getAsJsonObject("config").has("auth_timeout_ms"))
                            || entry.getKey().contains("auth-wait");
            if (declaresAuthTimeout) {
                assertThat(AckWaits.startsAuthWait(vector))
                        .as("%s: AUTH 대기 시작(서버의 ACK HELLO 송신이나 클라이언트의 AUTH 송신)을 해석할 수 없다", entry.getKey())
                        .isTrue();
                assertThat(AckWaits.hasAdvance(vector))
                        .as("%s: AUTH 대기 한도를 보려면 advance 단계가 있어야 한다", entry.getKey())
                        .isTrue();
            }
            if (AckWaits.startsAnyWait(vector) && AckWaits.hasAdvance(vector)) {
                assertThat(AckWaits.reachesLimitExactly(vector))
                        .as("%s: 한도에 정확히 닿으면 안 된다", entry.getKey())
                        .isFalse();

                assertThat(AckWaits.maxLimitsCrossedInOneAdvance(vector))
                        .as("%s: 한 단계에서 두 대기가 한도를 넘으면 안 된다", entry.getKey())
                        .isLessThanOrEqualTo(1);
            }
        }
        assertThat(declared).isGreaterThanOrEqualTo(11);
    }

    @Test
    void requestCheckOrderIsRefusedThenUnimplementedThenInvalidArgument() {
        for (String key : Arrays.asList(
                "request-checks/limit-refusal-precedes-unknown-type",
                "request-checks/limit-refusal-precedes-bad-extension-value")) {

            assertThat(sentFrames(vector(key))).as(key).containsExactly("RESET/REFUSED");
        }

        for (String key : Arrays.asList(
                "request-checks/closing-drops-unknown-type-request",
                "request-checks/closing-drops-bad-extension-value-request",
                "request-checks/closing-drops-unknown-type-with-bad-extension-value")) {
            assertThat(sentFrames(vector(key))).as(key).isEmpty();
        }

        assertThat(sentFrames(vector("request-checks/unknown-type-precedes-bad-extension-value")))
                .containsExactly("258/UNIMPLEMENTED");

        assertThat(sentFrames(vector("request-checks/timeout-length-three-is-invalid-argument")))
                .containsExactly("257/INVALID_ARGUMENT");
    }

    @Test
    void applicationCloseAfterAnyCloseSendsNothing() {
        for (String key : Arrays.asList(
                "close-races/application-close-after-close-sent-sends-nothing",
                "close-races/application-close-after-close-received-sends-nothing")) {
            JsonArray steps = vector(key).getAsJsonArray("steps");
            JsonObject last = steps.get(steps.size() - 1).getAsJsonObject();
            assertThat(last.get("kind").getAsString()).isEqualTo("close");
            assertThat(last.getAsJsonArray("send")).isEmpty();
        }
    }
}
