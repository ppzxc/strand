package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.OutboundMessage;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** 러너가 기대와 다른 동작을 놓치지 않는지 본다. 벡터를 직접 만들어 일부러 어긋나게 한다. */
class VectorRunnerTest {

    private static final String PING = "00000018 00020000 0000000000000000 00000000 0102030405060708";
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

    private static final String HELLO_ACK_SEND = "{\"type\":\"HELLO\",\"flags\":[\"ACK\"]}";
    private static final String CLOSE_PE =
            "{\"type\":\"CLOSE\",\"error_code\":\"PROTOCOL_ERROR\",\"last_stream_id\":\"0\"}";
    private static final String HELLO_RECV = "00000010000100000000000000000000" + "00000000";

    private static String step(String kind, String fields, String send, String events) {
        return "{\"kind\":\"" + kind + "\"," + fields + "\"send\":" + send + ",\"events\":" + events + "}";
    }

    @Test
    void clientOpeningOutputIsMergedIntoTheFirstStep() {
        ConformanceVector v = vector(
                "{\"role\":\"client\",\"established\":false}",
                recv(HELLO_RECV, "[{\"type\":\"PREFACE\"},{\"type\":\"HELLO\"}," + HELLO_ACK_SEND + "]", "[]"));

        assertThatCode(() -> VectorRunner.run(v)).doesNotThrowAnyException();
    }

    @Test
    void clientOpeningOutputMustBePresentInTheFirstStep() {
        ConformanceVector v = vector(
                "{\"role\":\"client\",\"established\":false}", recv(HELLO_RECV, "[" + HELLO_ACK_SEND + "]", "[]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void establishedClientStartsAfterTheStandardSequence() {
        ConformanceVector v = vector(
                "{\"role\":\"client\",\"established\":true}",
                recv(PING, String.format(PING_ACK_SEND, "0102030405060708"), "[]"));

        assertThatCode(() -> VectorRunner.run(v)).doesNotThrowAnyException();
    }

    @Test
    void advanceMovesTheVirtualClockAndTheAckTimeoutIsConfigurable() {
        ConformanceVector v = vector(
                "{\"role\":\"server\",\"established\":false,\"config\":{\"ack_timeout_ms\":1000}}",
                recv("c0de535452300000", "[{\"type\":\"HELLO\"}]", "[]"),
                recv(HELLO_RECV, "[" + HELLO_ACK_SEND + "]", "[]"),
                step("advance", "\"ms\":999,", "[]", "[]"),
                step("advance", "\"ms\":2,", "[" + CLOSE_PE + "]", "[{\"event\":\"closed\"}]"));

        assertThatCode(() -> VectorRunner.run(v)).doesNotThrowAnyException();
    }

    @Test
    void requestPingAndDisconnectStepsDriveTheEndpoint() {
        ConformanceVector v = vector(
                ESTABLISHED,
                step("request", "\"type\":257,", "[{\"type\":257,\"stream_id\":\"2\"}]", "[]"),
                step(
                        "ping",
                        "\"payload\":\"0102030405060708\",",
                        "[{\"type\":\"PING\",\"payload\":\"0102030405060708\"}]",
                        "[]"),
                step(
                        "disconnect",
                        "",
                        "[]",
                        "[{\"event\":\"result\",\"stream_id\":\"2\",\"status\":\"UNAVAILABLE\"},{\"event\":\"closed\"}]"));

        assertThatCode(() -> VectorRunner.run(v)).doesNotThrowAnyException();
    }

    @Test
    void anApplicationFrameWithTheWrongStreamIdFails() {
        ConformanceVector v =
                vector(ESTABLISHED, step("request", "\"type\":257,", "[{\"type\":257,\"stream_id\":\"4\"}]", "[]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void aWrongResultStatusFails() {
        ConformanceVector v = vector(
                ESTABLISHED,
                step("request", "\"type\":257,", "[{\"type\":257,\"stream_id\":\"2\"}]", "[]"),
                step(
                        "disconnect",
                        "",
                        "[]",
                        "[{\"event\":\"result\",\"stream_id\":\"2\",\"status\":\"INTERNAL\"},{\"event\":\"closed\"}]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void unsupportedSetupFailsInsteadOfBeingSkipped() {
        ConformanceVector v = vector(
                "{\"role\":\"server\",\"established\":true,\"config\":{\"unknown_key\":1}}", recv(PING, "[]", "[]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void unsupportedStepFailsInsteadOfBeingSkipped() {
        ConformanceVector v =
                vector(ESTABLISHED, step("request", "\"type\":257,\"extensions\":[{\"tag\":1}],", "[]", "[]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    private static final String WEBSOCKET_ESTABLISHED =
            "{\"role\":\"server\",\"established\":true,\"transport\":\"websocket\"}";
    private static final String WEBSOCKET_CLOSE_1000 = "{\"type\":\"WEBSOCKET_CLOSE\",\"code\":1000}";
    private static final String CLOSE_NO_ERROR =
            "{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":\"0\"}";
    private static final String CLOSE_STEP_NO_ERROR = "\"error_code\":\"NO_ERROR\",";

    @Test
    void webSocketCloseItemIsComparedAfterTheCloseFrame() {
        ConformanceVector v = vector(
                WEBSOCKET_ESTABLISHED,
                step(
                        "close",
                        CLOSE_STEP_NO_ERROR,
                        "[" + CLOSE_NO_ERROR + "," + WEBSOCKET_CLOSE_1000 + "]",
                        "[{\"event\":\"closed\"}]"));

        assertThatCode(() -> VectorRunner.run(v)).doesNotThrowAnyException();
    }

    @Test
    void aMissingWebSocketCloseItemFails() {
        ConformanceVector v = vector(
                WEBSOCKET_ESTABLISHED,
                step("close", CLOSE_STEP_NO_ERROR, "[" + CLOSE_NO_ERROR + "]", "[{\"event\":\"closed\"}]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void aWebSocketCloseItemBeforeTheCloseFrameFails() {
        ConformanceVector v = vector(
                WEBSOCKET_ESTABLISHED,
                step(
                        "close",
                        CLOSE_STEP_NO_ERROR,
                        "[" + WEBSOCKET_CLOSE_1000 + "," + CLOSE_NO_ERROR + "]",
                        "[{\"event\":\"closed\"}]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void aByteStreamEndpointExpectedToSendAWebSocketCloseFails() {
        ConformanceVector v = vector(
                ESTABLISHED,
                step(
                        "close",
                        CLOSE_STEP_NO_ERROR,
                        "[" + CLOSE_NO_ERROR + "," + WEBSOCKET_CLOSE_1000 + "]",
                        "[{\"event\":\"closed\"}]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    /** 규격이 정하지 않아 관측하지 않는 WebSocket Close(1000이 아닌 상태 코드). 엔드포인트는 내지 않으므로 직접 만든다. */
    private static OutboundMessage unobservedWebSocketClose() throws ReflectiveOperationException {
        Constructor<OutboundMessage> constructor =
                OutboundMessage.class.getDeclaredConstructor(OutboundMessage.Kind.class, byte[].class, int.class);
        constructor.setAccessible(true);
        return constructor.newInstance(OutboundMessage.Kind.WEBSOCKET_CLOSE, new byte[0], 1001);
    }

    private static OutboundMessage binaryMessage(byte[] bytes) throws ReflectiveOperationException {
        Method binary = OutboundMessage.class.getDeclaredMethod("binary", byte[].class);
        binary.setAccessible(true);
        return (OutboundMessage) binary.invoke(null, (Object) bytes);
    }

    @Test
    void skippingAnUnobservedWebSocketCloseKeepsTheNextMessageAsTheFirst() throws ReflectiveOperationException {
        VectorRunner runner = new VectorRunner(true, true, Endpoint.client(EndpointConfig.defaults()));
        List<OutboundMessage> messages =
                Arrays.asList(unobservedWebSocketClose(), binaryMessage(Bytes.fromHex("c0de535452300000")));

        assertThat(runner.describeMessages(messages, true)).containsExactly("PREFACE");
    }

    private static final String PING_FRAME = "00000018000200000000000000000000" + "00000000" + "0102030405060708";

    @Test
    void aWebSocketMessageCarryingTwoFramesFails() throws ReflectiveOperationException {
        VectorRunner runner = new VectorRunner(true, true, Endpoint.client(EndpointConfig.defaults()));
        List<OutboundMessage> messages = Arrays.asList(binaryMessage(Bytes.fromHex(PING_FRAME + PING_FRAME)));

        assertThatThrownBy(() -> runner.describeMessages(messages, false)).isInstanceOf(AssertionError.class);
    }

    @Test
    void aFrameSplitAcrossTwoWebSocketMessagesFails() throws ReflectiveOperationException {
        VectorRunner runner = new VectorRunner(true, true, Endpoint.client(EndpointConfig.defaults()));
        List<OutboundMessage> messages = Arrays.asList(
                binaryMessage(Bytes.fromHex(PING_FRAME.substring(0, 20))),
                binaryMessage(Bytes.fromHex(PING_FRAME.substring(20))));

        assertThatThrownBy(() -> runner.describeMessages(messages, false)).isInstanceOf(AssertionError.class);
    }

    @Test
    void aByteStreamMayCarryTwoFramesInOneMessageOrSplitAcrossMessages() throws ReflectiveOperationException {
        VectorRunner runner = new VectorRunner(true, false, Endpoint.client(EndpointConfig.defaults()));
        List<OutboundMessage> twoInOne = Arrays.asList(binaryMessage(Bytes.fromHex(PING_FRAME + PING_FRAME)));
        List<OutboundMessage> split = Arrays.asList(
                binaryMessage(Bytes.fromHex(PING_FRAME.substring(0, 20))),
                binaryMessage(Bytes.fromHex(PING_FRAME.substring(20) + PING_FRAME)));

        assertThat(runner.describeMessages(twoInOne, false)).hasSize(2);
        assertThat(runner.describeMessages(split, false)).hasSize(2);
    }
}
