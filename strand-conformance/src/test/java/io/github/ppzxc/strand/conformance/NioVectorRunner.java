package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.github.ppzxc.strand.nio.internal.PartialWriteObservation;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 전송 계층 쓰기 경계가 필요한 벡터를 NIO 연결 드라이버로 실행한다. */
final class NioVectorRunner {

    private NioVectorRunner() {}

    static boolean supports(ConformanceVector vector) {
        for (JsonObject step : vector.steps()) {
            if (step.has("write_limit_bytes")) return true;
        }
        return false;
    }

    static VectorOutcome evaluate(ConformanceVector vector, String vectorId) {
        if (!supports(vector))
            return VectorOutcome.unsupported(
                    vectorId, Collections.singletonList("NIO runner requires request.write_limit_bytes"));
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
        PartialWriteObservation.Result actual = PartialWriteObservation.cancelAfterPartialWrite(
                requestStep.get("type").getAsInt(),
                Bytes.fromHex(
                        requestStep.has("payload") ? requestStep.get("payload").getAsString() : ""),
                requestStep.get("write_limit_bytes").getAsInt());

        assertThat(cancelStep.getAsJsonArray("events")).hasSize(1);
        JsonObject expectedEvent = cancelStep.getAsJsonArray("events").get(0).getAsJsonObject();
        assertThat(expectedEvent.get("event").getAsString()).isEqualTo("result");
        assertThat(actual.status()).isEqualTo(Protocol.statusCode(expectedEvent.get("status")));
        assertThat(actual.certainty().name().toLowerCase(java.util.Locale.ROOT))
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
