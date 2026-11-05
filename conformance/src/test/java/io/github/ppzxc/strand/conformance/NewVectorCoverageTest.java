package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * #94 B절 신규 벡터가 빠짐없이 있는지 본다(#98). 항목별 필수 벡터 id는 {@code required-vectors.txt}에 있다. 벡터의 기대값은 이 테스트가
 * 아니라 벡터 파일과 규격이 정한다. 여기서는 존재와 인수 조건의 형태만 단언한다.
 */
class NewVectorCoverageTest {

    /** B-28과 B-32는 벡터를 만들지 않는다. */
    private static final List<Integer> EXCLUDED = Arrays.asList(28, 32);

    private static Map<Integer, List<String>> required() throws IOException {
        Map<Integer, List<String>> result = new LinkedHashMap<>();
        try (Reader reader = new InputStreamReader(
                NewVectorCoverageTest.class.getResourceAsStream("/required-vectors.txt"), StandardCharsets.UTF_8)) {
            for (String line : new BufferedReader(reader).lines().collect(Collectors.toList())) {
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] parts = line.split(" ");
                int n = Integer.parseInt(parts[0].substring("B-".length()));
                result.computeIfAbsent(n, k -> new ArrayList<>()).add(parts[1]);
            }
        }
        return result;
    }

    private static Map<String, JsonObject> allVectors() {
        return RawVectors.byKey();
    }

    private static JsonObject vector(String key) throws IOException {
        JsonObject vector = allVectors().get(key);
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
        Map<String, JsonObject> vectors = allVectors();
        for (Map.Entry<Integer, List<String>> entry : required().entrySet()) {
            for (String key : entry.getValue()) {
                assertThat(vectors).as("B-%d의 %s", entry.getKey(), key).containsKey(key);
            }
        }
    }

    @Test
    void aVectorUsesKnownTypesAndExpectsUnimplementedWithoutClosing() throws IOException {
        JsonObject v = vector("request-checks/unknown-type-request-is-answered-unimplemented");
        assertThat(v.getAsJsonObject("setup").has("known_types")).isTrue();
        // 모르는 type(258)의 요청에는 UNIMPLEMENTED 응답만 나가고 CLOSE도 연결 종료 이벤트도 없다.
        assertThat(sentFrames(v)).contains("258/UNIMPLEMENTED").noneMatch(f -> f.startsWith("CLOSE/"));
        assertThat(eventNames(v)).doesNotContain("closed").contains("request_received");
    }

    /** 벡터 전체 단계에서 구현이 보낸 프레임을 `type/status|error_code` 꼴로 모은다. 애플리케이션 프레임은 type이 숫자다. */
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
    void aVectorUsesTheWebSocketTransport() throws IOException {
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
    void aVectorUsesNextStreamIdForNumberExhaustion() throws IOException {
        JsonObject client = vector("streams/exhaustion-after-last-number-sends-close-client")
                .getAsJsonObject("setup");
        assertThat(client.get("next_stream_id").getAsLong()).isEqualTo(2147483647L);
        JsonObject server = vector("streams/exhaustion-after-last-number-sends-close-server")
                .getAsJsonObject("setup");
        assertThat(server.get("next_stream_id").getAsLong()).isEqualTo(2147483646L);
    }

    @Test
    void aVectorUsesPingStepWithAckTimeout() throws IOException {
        JsonObject v = vector("ack-timeout/ping-ack-timeout-before-and-after-limit");
        assertThat(v.getAsJsonObject("setup").getAsJsonObject("config").has("ack_timeout_ms"))
                .isTrue();
        List<String> kinds = new ArrayList<>();
        for (JsonElement step : v.getAsJsonArray("steps")) {
            kinds.add(step.getAsJsonObject().get("kind").getAsString());
        }
        assertThat(kinds).contains("ping", "advance");
    }

    /**
     * 확인 대기를 시작하고 시간을 흘리는 모든 벡터는 한도(`ack_timeout_ms`, 없으면 5000)에 정확히 닿지 않는다(형식 문서 8.3절).
     * `ack_timeout_ms`를 쓰거나 id에 ack-timeout이 있는 벡터는 확인 대기를 시작해야 한다.
     */
    @Test
    void timeVectorsNeverReachTheLimitExactly() throws IOException {
        int declared = 0;
        int timed = 0;
        for (Map.Entry<String, JsonObject> entry : allVectors().entrySet()) {
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
            }
            if (AckWaits.startsAnyWait(vector) && AckWaits.hasAdvance(vector)) {
                timed++;
                assertThat(AckWaits.reachesLimitExactly(vector))
                        .as("%s: 한도에 정확히 닿으면 안 된다", entry.getKey())
                        .isFalse();
            }
        }
        assertThat(declared).isGreaterThanOrEqualTo(11);
        assertThat(timed).isGreaterThanOrEqualTo(declared);
    }

    /** 5.1절 5번 (가)→(나)→(다). 어느 검사가 먼저 걸리는지는 송신 프레임으로 본다. */
    @Test
    void requestCheckOrderIsRefusedThenUnimplementedThenInvalidArgument() throws IOException {
        for (String key : Arrays.asList(
                "request-checks/closing-refuses-unknown-type-request",
                "request-checks/closing-refuses-bad-extension-value-request",
                "request-checks/closing-refuses-unknown-type-with-bad-extension-value",
                "request-checks/limit-refusal-precedes-unknown-type",
                "request-checks/limit-refusal-precedes-bad-extension-value")) {
            // (가) 거절만 나간다. (나)·(다)의 응답은 나가지 않는다.
            assertThat(sentFrames(vector(key))).as(key).containsExactly("RESET/REFUSED");
        }
        // (나)가 (다)보다 먼저 걸린다.
        assertThat(sentFrames(vector("request-checks/unknown-type-precedes-bad-extension-value")))
                .containsExactly("258/UNIMPLEMENTED");
        // (다) 단독 사례도 있어야 순서가 의미를 가진다.
        assertThat(sentFrames(vector("request-checks/timeout-length-three-is-invalid-argument")))
                .containsExactly("257/INVALID_ARGUMENT");
    }

    @Test
    void applicationCloseAfterAnyCloseSendsNothing() throws IOException {
        for (String key : Arrays.asList(
                "close-races/application-close-after-close-sent-sends-nothing",
                "close-races/application-close-after-close-received-sends-nothing")) {
            JsonArray steps = vector(key).getAsJsonArray("steps");
            JsonObject last = steps.get(steps.size() - 1).getAsJsonObject();
            assertThat(last.get("kind").getAsString()).isEqualTo("close");
            assertThat(last.getAsJsonArray("send")).isEmpty();
        }
    }

    @Test
    void newVectorSpecReferencesNeverUseRemovedSubsections() throws IOException {
        for (Map.Entry<Integer, List<String>> entry : required().entrySet()) {
            for (String key : entry.getValue()) {
                for (JsonElement ref : vector(key).getAsJsonArray("spec")) {
                    assertThat(ref.getAsString()).as(key).doesNotStartWith("5.1.");
                }
            }
        }
    }
}
