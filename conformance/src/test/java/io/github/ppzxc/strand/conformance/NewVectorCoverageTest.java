package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

    /** `파일명/id`에서 벡터 객체로. */
    private static Map<String, JsonObject> allVectors() throws IOException {
        Map<String, JsonObject> result = new LinkedHashMap<>();
        List<Path> files;
        try (Stream<Path> list = Files.list(VectorSuite.defaultDirectory())) {
            files = list.filter(p -> p.toString().endsWith(".json")).sorted().collect(Collectors.toList());
        }
        for (Path file : files) {
            String name = file.getFileName().toString().replace(".json", "");
            try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                for (JsonElement v :
                        JsonParser.parseReader(reader).getAsJsonObject().getAsJsonArray("vectors")) {
                    result.put(name + "/" + v.getAsJsonObject().get("id").getAsString(), v.getAsJsonObject());
                }
            }
        }
        return result;
    }

    private static JsonObject vector(String key) throws IOException {
        JsonObject vector = allVectors().get(key);
        assertThat(vector).as(key).isNotNull();
        return vector;
    }

    private static String text(String key) throws IOException {
        return vector(key).toString();
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
        String json = v.toString();
        assertThat(json).contains("\"status\":\"UNIMPLEMENTED\"").doesNotContain("\"CLOSE\"");
        assertThat(json).contains("request_received");
    }

    @Test
    void aVectorUsesTheWebSocketTransport() throws IOException {
        JsonObject setup = vector("websocket/established-ping-is-acknowledged").getAsJsonObject("setup");
        assertThat(setup.get("transport").getAsString()).isEqualTo("websocket");
        assertThat(text("websocket/established-ping-is-acknowledged")).contains("recv_message");
    }

    @Test
    void aVectorUsesNextStreamIdForNumberExhaustion() throws IOException {
        JsonObject client = vector("stream-numbers/exhaustion-after-last-number-sends-close-client")
                .getAsJsonObject("setup");
        assertThat(client.get("next_stream_id").getAsLong()).isEqualTo(2147483647L);
        JsonObject server = vector("stream-numbers/exhaustion-after-last-number-sends-close-server")
                .getAsJsonObject("setup");
        assertThat(server.get("next_stream_id").getAsLong()).isEqualTo(2147483646L);
    }

    @Test
    void aVectorUsesPingStepWithAckTimeout() throws IOException {
        JsonObject v = vector("ack-timeout/ping-ack-timeout-before-and-after-limit");
        assertThat(v.getAsJsonObject("setup").getAsJsonObject("config").has("ack_timeout_ms"))
                .isTrue();
        assertThat(v.toString()).contains("\"kind\":\"ping\"").contains("\"kind\":\"advance\"");
    }

    @Test
    void timeVectorsNeverReachTheLimitExactly() throws IOException {
        for (Map.Entry<String, JsonObject> entry : allVectors().entrySet()) {
            JsonObject setup = entry.getValue().getAsJsonObject("setup");
            if (!entry.getKey().startsWith("ack-timeout/") || !setup.has("config")) {
                continue;
            }
            long limit = setup.getAsJsonObject("config").get("ack_timeout_ms").getAsLong();
            long clock = 0;
            for (JsonElement step : entry.getValue().getAsJsonArray("steps")) {
                JsonObject s = step.getAsJsonObject();
                if (s.get("kind").getAsString().equals("advance")) {
                    clock += s.get("ms").getAsLong();
                    assertThat(clock).as("%s: 한도에 정확히 닿으면 안 된다", entry.getKey()).isNotEqualTo(limit);
                }
            }
        }
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
            assertThat(text(key)).as(key).contains("\"error_code\":\"REFUSED\"");
            assertThat(text(key)).as(key).doesNotContain("UNIMPLEMENTED").doesNotContain("INVALID_ARGUMENT");
        }
        String nd = text("request-checks/unknown-type-precedes-bad-extension-value");
        assertThat(nd).contains("UNIMPLEMENTED").doesNotContain("INVALID_ARGUMENT").doesNotContain("REFUSED");
        // (다) 단독 사례도 있어야 순서가 의미를 가진다.
        assertThat(text("request-checks/timeout-length-three-is-invalid-argument")).contains("INVALID_ARGUMENT");
    }

    @Test
    void applicationCloseAfterAnyCloseSendsNothing() throws IOException {
        for (String key : Arrays.asList(
                "close-handling/application-close-after-close-sent-sends-nothing",
                "close-handling/application-close-after-close-received-sends-nothing")) {
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
