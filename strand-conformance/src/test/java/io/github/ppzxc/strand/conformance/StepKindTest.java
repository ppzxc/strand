package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

/** 단계 종류 정의의 내부 불변식: 형태·실행·지원 여부·타이밍 성질이 한 정의에서 나온다. */
class StepKindTest {

    private static JsonObject json(String text) {
        return JsonParser.parseString(text).getAsJsonObject();
    }

    private static StepKind kind(String name) {
        return StepKind.of(json("{\"kind\":\"" + name + "\"}"));
    }

    @Test
    void shapesAreDerivedFromTheDefinitions() {
        assertThat(StepKind.shapesByName().keySet())
                .containsExactlyInAnyOrder(
                        "recv", "recv_message", "ping", "request", "respond", "cancel", "close", "advance",
                        "disconnect");
        assertThat(StepKind.shapesByName().get("advance").names())
                .containsExactlyInAnyOrder("kind", "send", "events", "ms");
    }

    @Test
    void everyDefinedKindIsSupported() {
        for (String name : StepKind.shapesByName().keySet()) {
            assertThat(kind(name).isSupported()).as(name).isTrue();
        }
    }

    @Test
    void timingTraitsAreDefinedPerKind() {
        assertThat(kind("advance").advancesTime()).isTrue();
        assertThat(kind("request").advancesTime()).isFalse();
        assertThat(kind("disconnect").endsConnection()).isTrue();
        assertThat(kind("close").endsConnection()).isFalse();
        assertThat(kind("recv").inbound()).isEqualTo(StepKind.Inbound.STREAM);
        assertThat(kind("recv_message").inbound()).isEqualTo(StepKind.Inbound.MESSAGE);
        assertThat(kind("request").inbound()).isEqualTo(StepKind.Inbound.NONE);
        assertThat(kind("ping").sendsPing()).isTrue();
    }

    @Test
    void transportRestrictionIsDefinedPerKind() {
        assertThat(kind("recv").onlyOnTransport()).isEqualTo("tcp");
        assertThat(kind("recv_message").onlyOnTransport()).isEqualTo("websocket");
        assertThat(kind("cancel").onlyOnTransport()).isNull();
    }

    @Test
    void unknownKindIsRejected() {
        assertThatThrownBy(() -> kind("nope")).isInstanceOf(IllegalArgumentException.class);
    }
}
