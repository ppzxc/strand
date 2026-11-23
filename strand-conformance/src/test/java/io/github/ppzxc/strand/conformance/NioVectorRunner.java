package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.github.ppzxc.strand.nio.internal.PartialWriteObservation;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** 전송 계층 쓰기 경계가 필요한 벡터를 제품 NIO 연결 엔진으로 실행한다. */
final class NioVectorRunner {

    private NioVectorRunner() {}

    static List<String> unsupportedFeatures(ConformanceVector vector) {
        List<String> unsupported = new ArrayList<>();
        JsonObject setup = vector.setup();
        if (!"client".equals(setup.get("role").getAsString())) {
            unsupported.add("setup.role");
        }
        if (!setup.get("established").getAsBoolean()) {
            unsupported.add("setup.established");
        }
        Set<String> supportedSetup = new HashSet<>(Arrays.asList("role", "established", "transport"));
        for (String key : setup.keySet()) {
            if (!supportedSetup.contains(key)) {
                unsupported.add("setup." + key);
            }
        }
        if (setup.has("transport")
                && !VectorSchema.TCP.equals(setup.get("transport").getAsString())) {
            unsupported.add("setup.transport");
        }

        List<JsonObject> steps = vector.steps();
        if (steps.size() != 2) {
            unsupported.add("steps: NIO vector requires one request and one cancel");
        }
        if (steps.isEmpty()
                || !StepKind.REQUEST_NAME.equals(steps.get(0).get(StepKind.TAG).getAsString())) {
            unsupported.add("steps[0]: request required");
        } else {
            JsonObject request = steps.get(0);
            for (String unsupportedRequestFeature : StepKind.of(request).unsupportedParts(request)) {
                if (!"request.write_limit_bytes".equals(unsupportedRequestFeature)) {
                    unsupported.add(unsupportedRequestFeature);
                }
            }
            if (!request.has("write_limit_bytes")) {
                unsupported.add("request.write_limit_bytes");
            }
            Set<String> supportedRequest = new HashSet<>(Arrays.asList(
                    "kind", "type", "name", "write_limit_bytes", "payload", "extensions", "send", "events"));
            for (String key : request.keySet()) {
                if (!supportedRequest.contains(key)) {
                    unsupported.add("request." + key);
                }
            }
            if (!request.has("name")) {
                unsupported.add("request.name");
            }
            if (!request.getAsJsonArray("send").isEmpty()
                    || !request.getAsJsonArray("events").isEmpty()) {
                unsupported.add("request expectations must be empty until cancel");
            }
            if (request.has("write_limit_bytes")) {
                if (request.get("write_limit_bytes").getAsLong() >= VectorSchema.requestFrameSize(request)) {
                    unsupported.add("request.write_limit_bytes must stop inside the request frame");
                }
            }
        }
        if (steps.size() > 1) {
            JsonObject cancel = steps.get(1);
            if (!StepKind.CANCEL_NAME.equals(cancel.get(StepKind.TAG).getAsString())) {
                unsupported.add("steps[1]: cancel required");
            } else {
                if (cancel.has("stream_id") || !cancel.has("request")) {
                    unsupported.add("cancel.request");
                } else if (!steps.isEmpty()
                        && steps.get(0).has("name")
                        && !steps.get(0).get("name").equals(cancel.get("request"))) {
                    unsupported.add("cancel.request must name the request step");
                }
                Set<String> supportedCancel = new HashSet<>(Arrays.asList("kind", "request", "send", "events"));
                for (String key : cancel.keySet()) {
                    if (!supportedCancel.contains(key)) {
                        unsupported.add("cancel." + key);
                    }
                }
            }
        }
        return Collections.unmodifiableList(unsupported);
    }

    static VectorOutcome evaluate(ConformanceVector vector, String vectorId) {
        List<String> unsupported = unsupportedFeatures(vector);
        if (!unsupported.isEmpty()) {
            return VectorOutcome.unsupported(vectorId, unsupported);
        }
        try {
            runPartialWriteCancellation(vector);
            return VectorOutcome.pass(vectorId);
        } catch (AssertionError | Exception failure) {
            return VectorOutcome.fail(vectorId, failure.getMessage());
        }
    }

    private static void runPartialWriteCancellation(ConformanceVector vector) throws Exception {
        List<JsonObject> steps = vector.steps();
        assertThat(steps).hasSize(2);
        JsonObject requestStep = steps.get(0);
        JsonObject cancelStep = steps.get(1);
        assertThat(requestStep.getAsJsonArray("send")).isEmpty();
        assertThat(requestStep.getAsJsonArray("events")).isEmpty();
        PartialWriteObservation.Result actual = PartialWriteObservation.cancelAfterPartialWrite(
                requestStep.get("type").getAsInt(),
                Bytes.fromHex(VectorSchema.hexOrEmpty(requestStep, "payload")),
                requestStep.get("write_limit_bytes").getAsInt());

        assertThat(cancelStep.getAsJsonArray("events")).hasSize(1);
        JsonObject expectedEvent = cancelStep.getAsJsonArray("events").get(0).getAsJsonObject();
        assertThat(expectedEvent.get("event").getAsString()).isEqualTo("result");
        assertThat(expectedEvent.has("stream_id")).isTrue();
        assertThat(actual.resultDeliveries()).isEqualTo(1);
        assertThat(actual.frames()).isNotEmpty();
        assertThat(Protocol.streamIdFromFrame(actual.frames().get(0)))
                .isEqualTo(expectedEvent.get("stream_id").getAsString());
        assertThat(actual.status()).isEqualTo(Protocol.statusCode(expectedEvent.get("status")));
        assertThat(actual.certainty().name().toLowerCase(Locale.ROOT))
                .isEqualTo(expectedEvent.get("certainty").getAsString());

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (byte[] frame : actual.frames()) output.write(frame);
        List<String> actualFrames = FrameKind.describe(output.toByteArray(), false, true);
        List<String> expectedFrames = new ArrayList<>();
        JsonArray sends = cancelStep.getAsJsonArray("send");
        for (int i = 0; i < sends.size(); i++) {
            expectedFrames.add(FrameKind.expected(sends.get(i).getAsJsonObject()));
        }
        assertThat(actualFrames).containsExactlyElementsOf(expectedFrames);
    }
}
