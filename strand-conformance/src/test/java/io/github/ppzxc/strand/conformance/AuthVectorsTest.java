package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AuthVectorsTest {

    private static final List<String> REQUIRED = Arrays.asList(
            "auth/server-auth-anonymous-passes-and-gate-opens",
            "auth/server-auth-with-credentials-passes",
            "auth/client-auth-passes-and-gate-opens",
            "auth/client-auth-ack-ignores-unknown-identifier",
            "auth/server-wrong-credentials-closes-unauthenticated",
            "auth/server-unknown-mechanism-closes-unauthenticated",
            "auth/server-auth-wait-times-out-closes-unauthenticated",
            "auth/server-auth-wait-is-cancelled-by-auth",
            "auth/client-receives-unauthenticated-close",
            "auth/client-auth-ack-wait-times-out-closes-unresponsive",
            "auth/server-ping-before-auth-closes",
            "auth/server-reset-before-auth-closes",
            "auth/server-application-frame-before-auth-closes",
            "auth/client-ping-before-auth-ack-closes",
            "auth/client-application-frame-before-auth-ack-closes",
            "auth/server-sends-no-request-before-auth-passes",
            "auth/client-sends-no-request-before-auth-ack",
            "auth/auth-before-peer-ack-closes",
            "auth/server-auth-empty-payload-closes",
            "auth/server-auth-mechanism-length-zero-closes",
            "auth/server-auth-mechanism-length-above-twenty-closes",
            "auth/server-auth-mechanism-longer-than-payload-closes",
            "auth/server-auth-mechanism-with-lowercase-closes",
            "auth/server-auth-stream-id-nonzero-closes",
            "auth/client-auth-ack-payload-not-multiple-of-six-closes",
            "auth/client-auth-ack-duplicate-identifier-closes",
            "auth/server-receives-auth-ack-closes",
            "auth/client-receives-auth-closes",
            "auth/server-second-auth-closes",
            "auth/client-second-auth-ack-closes",
            "auth/websocket-auth-passes",
            "auth/server-auth-transient-failure-closes-internal-error",
            "auth/server-auth-revoked-closes-unauthenticated",
            "auth/server-auth-expiry-closes-no-error-after-lifetime",
            "auth/server-auth-expired-notification-closes-no-error",
            "auth/server-auth-expired-before-revoked-first-notification-wins",
            "auth/server-auth-revoked-before-expired-first-notification-wins");

    private static JsonObject vector(String key) {
        JsonObject vector = RawVectors.byKey().get(key);
        assertThat(vector).as(key).isNotNull();
        return vector;
    }

    @Test
    void everyRequiredAuthVectorExists() {
        for (String key : REQUIRED) {
            vector(key);
        }
    }

    @Test
    void theAuthFileHoldsNothingBesidesTheRequiredVectors() {
        List<String> inFile = new ArrayList<>();
        for (String key : RawVectors.byKey().keySet()) {
            if (key.startsWith("auth/")) {
                inFile.add(key);
            }
        }
        assertThat(inFile).containsExactlyInAnyOrderElementsOf(REQUIRED);
    }

    private static final Map<String, String> FRAME_CHECK_SECTIONS = new HashMap<>();

    static {
        FRAME_CHECK_SECTIONS.put("auth/client-receives-unauthenticated-close", "3.5");
        FRAME_CHECK_SECTIONS.put("auth/server-auth-stream-id-nonzero-closes", "2.2.5");
    }

    @Test
    void everyAuthVectorPointsToTheAuthSectionsOrTheFramesItChecks() {
        RequirementIndex index = RequirementIndex.ofSpecification();
        for (String key : REQUIRED) {
            List<String> sections = index.sectionsOf(key, RawVectors.requirementsOf(vector(key)));
            if (FRAME_CHECK_SECTIONS.containsKey(key)) {
                assertThat(sections).as(key).contains(FRAME_CHECK_SECTIONS.get(key));
            } else {
                assertThat(sections).as(key).anyMatch(section -> section.startsWith("3.3"));
            }
        }
    }

    @Test
    void theWebSocketAuthVectorUsesBinaryMessages() {
        JsonObject v = vector("auth/websocket-auth-passes");
        assertThat(v.getAsJsonObject("setup").get("transport").getAsString()).isEqualTo("websocket");
        for (JsonElement step : v.getAsJsonArray("steps")) {
            assertThat(step.getAsJsonObject().get("kind").getAsString()).isEqualTo("recv_message");
        }
    }

    @Test
    void theAuthWaitTimeoutVectorUsesAuthTimeoutMsAndTheClockOnly() {
        JsonObject v = vector("auth/server-auth-wait-times-out-closes-unauthenticated");
        assertThat(v.getAsJsonObject("setup").getAsJsonObject("config").has("auth_timeout_ms"))
                .isTrue();
        JsonObject last = v.getAsJsonArray("steps")
                .get(v.getAsJsonArray("steps").size() - 1)
                .getAsJsonObject();
        assertThat(last.get("kind").getAsString()).isEqualTo("advance");
        assertThat(last.getAsJsonArray("send")
                        .get(0)
                        .getAsJsonObject()
                        .get("error_code")
                        .getAsString())
                .isEqualTo("UNAUTHENTICATED");
    }

    @Test
    void rejectionVectorsSetTheAcceptedCredentialsInSetupAuth() {
        JsonObject setup = vector("auth/server-wrong-credentials-closes-unauthenticated")
                .getAsJsonObject("setup")
                .getAsJsonObject("auth");
        assertThat(setup.get("mechanism").getAsString()).isEqualTo("PLAIN");
        assertThat(setup.get("credentials").getAsString()).isNotEmpty();
    }

    @Test
    void noVectorTreatsType0005AsAReservedControlType() {
        Map<String, JsonObject> all = RawVectors.byKey();
        assertThat(all).doesNotContainKey("control-frames/reserved-type-0005-is-ignored");
        assertThat(all).containsKey("control-frames/reserved-type-0006-is-ignored");

        assertThat(typeAt("control-frames/reserved-type-0006-is-ignored", 0)).isEqualTo("0006");
        assertThat(typeAt("hello-establishment/ack-gate-allows-reserved-control-type", 2))
                .isEqualTo("0006");
        assertThat(typeAt("hello-establishment/server-first-frame-reserved-control-type-closes-silently", 1))
                .isEqualTo("0006");
    }

    private static String typeAt(String key, int step) {
        String hex = vector(key)
                .getAsJsonArray("steps")
                .get(step)
                .getAsJsonObject()
                .get("hex")
                .getAsString();
        return Bytes.compactHex(hex).substring(8, 12);
    }

    @Test
    void vectorsThatNoLongerHoldAreGone() {
        Map<String, JsonObject> all = RawVectors.byKey();
        for (String key : Arrays.asList(
                "hello-establishment/server-sends-after-acking-client-hello-without-waiting-for-ack",
                "hello-establishment/server-sends-ping-after-acking-client-hello",
                "hello-establishment/ack-gate-while-closing-closes-without-close-frame",
                "ack-timeout/hello-ack-timeout-while-closing-closes-without-close-frame")) {
            assertThat(all).as(key).doesNotContainKey(key);
        }
    }

    @Test
    void everyClientStepThatSendsHelloAckSendsAuthRightAfter() {
        int seen = 0;
        for (Map.Entry<String, JsonObject> entry : RawVectors.byKey().entrySet()) {
            JsonObject setup = entry.getValue().getAsJsonObject("setup");
            if (!"client".equals(setup.get("role").getAsString())) {
                continue;
            }
            for (JsonElement step : entry.getValue().getAsJsonArray("steps")) {
                List<String> sent = new ArrayList<>();
                for (JsonElement f : step.getAsJsonObject().getAsJsonArray("send")) {
                    JsonObject frame = f.getAsJsonObject();
                    String type = frame.get("type").getAsString();
                    boolean ack = VectorSchema.hasFlag(frame, "ACK");
                    sent.add(type + (ack ? "(ACK)" : ""));
                }
                int at = sent.indexOf("HELLO(ACK)");
                if (at >= 0) {
                    seen++;
                    assertThat(sent.subList(at + 1, sent.size()))
                            .as(entry.getKey())
                            .startsWith("AUTH");
                }
            }
        }
        assertThat(seen).isGreaterThanOrEqualTo(10);
    }

    @Test
    void aServerNeverSendsAnythingButAuthAckBeforeTheGateOpens() {
        int seen = 0;
        for (Map.Entry<String, JsonObject> entry : RawVectors.byKey().entrySet()) {
            JsonObject setup = entry.getValue().getAsJsonObject("setup");
            if (!"server".equals(setup.get("role").getAsString())
                    || setup.get("established").getAsBoolean()) {
                continue;
            }
            boolean gateOpen = false;
            for (JsonElement step : entry.getValue().getAsJsonArray("steps")) {
                for (JsonElement f : step.getAsJsonObject().getAsJsonArray("send")) {
                    JsonObject frame = f.getAsJsonObject();
                    String type = frame.get("type").getAsString();
                    boolean ack = VectorSchema.hasFlag(frame, "ACK");
                    if (type.equals("AUTH") && ack) {
                        gateOpen = true;
                        seen++;
                        continue;
                    }
                    if (!gateOpen) {
                        assertThat(type)
                                .as("%s: AUTH 관문 통과 전에 서버가 보낸 프레임", entry.getKey())
                                .isIn("PREFACE", "HELLO", "CLOSE", "WEBSOCKET_CLOSE");
                    }
                }
            }
        }
        assertThat(seen).isGreaterThanOrEqualTo(3);
    }
}
