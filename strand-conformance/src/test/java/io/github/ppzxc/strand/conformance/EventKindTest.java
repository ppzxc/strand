package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** 이벤트 종류 정의의 내부 불변식: 형태 표와 기대 렌더링이 같은 정의에서 나온다. */
class EventKindTest {

    private static JsonObject json(String text) {
        return JsonParser.parseString(text).getAsJsonObject();
    }

    @Test
    void shapesAreDerivedFromTheDefinitions() {
        assertThat(EventKind.shapesByName().keySet()).containsExactlyInAnyOrder("result", "request_received", "closed");
        assertThat(EventKind.shapesByName().get("result").names())
                .containsExactlyInAnyOrder("event", "stream_id", "status", "certainty");
    }

    @Test
    void everyDefinedKindIsSupportedAndOthersAreNot() {
        assertThat(Arrays.asList("result", "request_received", "closed")).allMatch(EventKind::isSupported);
        assertThat(EventKind.isSupported("authenticated")).isFalse();
    }

    @Test
    void localResultIsRenderedWithoutStreamId() {
        assertThat(EventKind.expected(
                        json("{\"event\":\"result\",\"status\":\"UNAVAILABLE\",\"certainty\":\"not_processed\"}")))
                .isEqualTo("result stream_id=- status=UNAVAILABLE certainty=not_processed");
    }

    @Test
    void resultWithStreamIdAndNumericStatus() {
        assertThat(EventKind.expected(
                        json("{\"event\":\"result\",\"stream_id\":\"3\",\"status\":0,\"certainty\":\"response\"}")))
                .isEqualTo("result stream_id=3 status=OK certainty=response");
    }

    @Test
    void renderingIgnoresFieldsOutsideTheVectorFormat() {
        assertThat(EventKind.expected(json("{\"event\":\"closed\",\"extra\":1}")))
                .isEqualTo("closed");
        assertThat(EventKind.expected(
                        json("{\"event\":\"request_received\",\"stream_id\":\"2\",\"type\":256,\"x\":1}")))
                .isEqualTo("request_received stream_id=2 type=256");
    }
}
