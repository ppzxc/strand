package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

/** 러너가 기대와 다른 동작을 놓치지 않는지 본다. 벡터를 직접 만들어 일부러 어긋나게 한다. */
class VectorRunnerTest {

    private static final String PING = "00000014 00020000 00000000 00000000 0102030405060708";
    private static final String PING_ACK_SEND = "[{\"type\":\"PING\",\"flags\":[\"ACK\"],\"payload\":\"%s\"}]";

    private static ConformanceVector vector(String setup, String... steps) {
        JsonObject setupJson = JsonParser.parseString(setup).getAsJsonObject();
        List<JsonObject> list = new ArrayList<>();
        for (String step : steps) {
            list.add(JsonParser.parseString(step).getAsJsonObject());
        }
        return new ConformanceVector("v", "d", Arrays.asList("3.4"), setupJson, list);
    }

    private static String recv(String hex, String send, String events) {
        return "{\"kind\":\"recv\",\"hex\":\"" + hex + "\",\"send\":" + send + ",\"events\":" + events + "}";
    }

    private static final String ESTABLISHED = "{\"role\":\"server\",\"established\":true}";

    @Test
    void passesWhenSendAndEventsMatch() {
        ConformanceVector v = vector(ESTABLISHED, recv(PING, String.format(PING_ACK_SEND, "0102030405060708"), "[]"));

        assertThatCode(() -> VectorRunner.run(v)).doesNotThrowAnyException();
    }

    @Test
    void failsWhenAFrameFieldDiffers() {
        ConformanceVector v = vector(ESTABLISHED, recv(PING, String.format(PING_ACK_SEND, "0102030405060709"), "[]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void failsWhenTheEndpointSendsSomethingNotExpected() {
        ConformanceVector v = vector(ESTABLISHED, recv(PING, "[]", "[]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void failsWhenAnExpectedFrameIsMissing() {
        ConformanceVector v = vector(ESTABLISHED, recv("", String.format(PING_ACK_SEND, "0102030405060708"), "[]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void failsWhenAnExpectedEventIsMissing() {
        ConformanceVector v = vector(
                ESTABLISHED, recv(PING, String.format(PING_ACK_SEND, "0102030405060708"), "[{\"event\":\"closed\"}]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void failsWhenAnEventIsNotExpected() {
        ConformanceVector v = vector(ESTABLISHED, recv("c1de535452300000", "[]", "[]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void matchesAnUnestablishedStartAndLocalHelloWithDefaultsFilledIn() {
        ConformanceVector v = vector(
                "{\"role\":\"server\",\"established\":false,\"local_hello\":{\"max_frame_size\":16384}}",
                recv("c0de535452300000", "[{\"type\":\"HELLO\",\"params\":{\"max_frame_size\":16384}}]", "[]"));

        assertThatCode(() -> VectorRunner.run(v)).doesNotThrowAnyException();
    }

    @Test
    void unsupportedSetupIsSkippedNotPassed() {
        ConformanceVector v = vector("{\"role\":\"client\",\"established\":true}", recv(PING, "[]", "[]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(TestAbortedException.class);
    }

    @Test
    void unsupportedStepIsSkippedNotPassed() {
        ConformanceVector v = vector(ESTABLISHED, "{\"kind\":\"advance\",\"ms\":1,\"send\":[],\"events\":[]}");

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(TestAbortedException.class);
    }
}
