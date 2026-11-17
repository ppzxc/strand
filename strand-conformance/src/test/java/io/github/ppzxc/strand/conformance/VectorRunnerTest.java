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
        return new ConformanceVector("v", "d", Arrays.asList("req-ping-ack-reply"), setupJson, list);
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
    private static final String CLOSE_UNRESPONSIVE =
            "{\"type\":\"CLOSE\",\"error_code\":\"UNRESPONSIVE\",\"last_stream_id\":\"0\"}";
    private static final String HELLO_RECV = "00000010000100000000000000000000" + "00000000";

    private static String step(String kind, String fields, String send, String events) {
        return "{\"kind\":\"" + kind + "\"," + fields + "\"send\":" + send + ",\"events\":" + events + "}";
    }

    @Test
    void clientOpeningOutputIsMergedIntoTheFirstStep() {
        ConformanceVector v = vector(
                "{\"role\":\"client\",\"established\":false}",
                recv(
                        HELLO_RECV,
                        "[{\"type\":\"PREFACE\"},{\"type\":\"HELLO\"}," + HELLO_ACK_SEND
                                + ",{\"type\":\"AUTH\",\"mechanism\":\"ANONYMOUS\"}]",
                        "[]"));

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
                step("advance", "\"ms\":2,", "[" + CLOSE_UNRESPONSIVE + "]", "[{\"event\":\"closed\"}]"));

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
                        "[{\"event\":\"result\",\"stream_id\":\"2\",\"status\":\"UNAVAILABLE\",\"certainty\":\"unknown\"},{\"event\":\"closed\"}]"));

        assertThatCode(() -> VectorRunner.run(v)).doesNotThrowAnyException();
    }

    @Test
    void anApplicationFrameWithTheWrongStreamIdFails() {
        ConformanceVector v =
                vector(ESTABLISHED, step("request", "\"type\":257,", "[{\"type\":257,\"stream_id\":\"4\"}]", "[]"));

        assertThatThrownBy(() -> VectorRunner.run(v)).isInstanceOf(AssertionError.class);
    }

    @Test
    void aWrongResultCertaintyFails() {
        ConformanceVector v = vector(
                ESTABLISHED,
                step("request", "\"type\":257,", "[{\"type\":257,\"stream_id\":\"2\"}]", "[]"),
                step(
                        "disconnect",
                        "",
                        "[]",
                        "[{\"event\":\"result\",\"stream_id\":\"2\",\"status\":\"UNAVAILABLE\",\"certainty\":\"not_processed\"},{\"event\":\"closed\"}]"));

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
                        "[{\"event\":\"result\",\"stream_id\":\"2\",\"status\":\"INTERNAL\",\"certainty\":\"unknown\"},{\"event\":\"closed\"}]"));

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

    @Test
    void unsupportedFeaturesAreReportedTogetherBeforeExecution() {
        ConformanceVector v = vector(
                "{\"role\":\"server\",\"established\":true,\"future_setup\":true}",
                step("future_step", "", "[]", "[]"));

        assertThat(VectorRunner.unsupportedFeatures(v))
                .containsExactly("setup.future_setup", "단계 future_step");
    }

    @Test
    void unsupportedVectorHasAnUnsupportedOutcomeInsteadOfAnExecutionFailure() {
        ConformanceVector v = vector(
                "{\"role\":\"server\",\"established\":true,\"future_setup\":true}",
                step("recv", "\"hex\":\"00\",", "[]", "[]"));

        VectorOutcome outcome = VectorRunner.evaluate(v);

        assertThat(outcome.status()).isEqualTo(VectorOutcome.Status.UNSUPPORTED);
        assertThat(outcome.vectorId()).isEqualTo("v");
        assertThat(outcome.details()).containsExactly("setup.future_setup");
    }

    @Test
    void anExecutableVectorMismatchHasAFailureOutcome() {
        ConformanceVector v = vector(ESTABLISHED, recv(PING, "[]", "[]"));

        assertThat(VectorRunner.evaluate(v).status()).isEqualTo(VectorOutcome.Status.FAIL);
    }

    @Test
    void reportCountsOutcomesAndFailsOnFailureOrUnsupported() {
        ConformanceReport report = ConformanceReport.of(Arrays.asList(
                VectorOutcome.pass("passed"),
                VectorOutcome.fail("failed", "mismatch"),
                VectorOutcome.unsupported("skipped", Arrays.asList("setup.future"))));

        assertThat(report.count(VectorOutcome.Status.PASS)).isEqualTo(1);
        assertThat(report.count(VectorOutcome.Status.FAIL)).isEqualTo(1);
        assertThat(report.count(VectorOutcome.Status.UNSUPPORTED)).isEqualTo(1);
        assertThat(report.passes()).isFalse();
        assertThat(report.summary()).contains("failed FAIL", "skipped UNSUPPORTED");
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

    // ---- 송신 프레임의 status·reserved·CLOSE reason (벡터 형식 6.1절) ----

    private static final String PING_EXPECTED = "{\"type\":\"PING\",\"payload\":\"0102030405060708\"}";

    /** 헤더 20바이트 + payload. reservedHex는 헤더 끝 2바이트, statusHex는 status 1바이트. */
    private static String frameHex(
            String length, String type, String statusHex, String streamId, String reservedHex, String payload) {
        return length + type + "00" + statusHex + streamId + "0000" + reservedHex + payload;
    }

    private static List<String> sentOnce(boolean client, String hex) throws ReflectiveOperationException {
        VectorRunner runner = new VectorRunner(client, false, Endpoint.client(EndpointConfig.defaults()));
        return runner.describeMessages(Arrays.asList(binaryMessage(Bytes.fromHex(hex))), false);
    }

    private static String expectedOf(String frameJson) {
        return FrameKind.expected(JsonParser.parseString(frameJson).getAsJsonObject());
    }

    @Test
    void aCleanPingMatchesItsExpectedFrame() throws ReflectiveOperationException {
        String hex = frameHex("00000018", "0002", "00", "0000000000000000", "0000", "0102030405060708");

        assertThat(sentOnce(true, hex)).containsExactly(expectedOf(PING_EXPECTED));
    }

    @Test
    void aNonZeroReservedOnAnySentFrameFails() throws ReflectiveOperationException {
        String ping = frameHex("00000018", "0002", "00", "0000000000000000", "0001", "0102030405060708");
        String request = frameHex("00000010", "0100", "00", "0000000000000001", "8000", "");

        assertThat(sentOnce(true, ping)).singleElement().asString().endsWith(" reserved=0x0001");
        assertThat(sentOnce(true, request)).singleElement().asString().endsWith(" reserved=0x8000");
    }

    @Test
    void aNonZeroStatusOnAControlFrameFails() throws ReflectiveOperationException {
        String ping = frameHex("00000018", "0002", "01", "0000000000000000", "0000", "0102030405060708");

        assertThat(sentOnce(true, ping)).singleElement().asString().endsWith(" sent_status=1");
    }

    @Test
    void aNonZeroReservedOrStatusOnAnUnreadableFrameFails() throws ReflectiveOperationException {
        String truncatedClose = frameHex("00000010", "0003", "02", "0000000000000000", "0001", "");

        assertThat(sentOnce(true, truncatedClose))
                .singleElement()
                .asString()
                .contains("reserved=0x0001")
                .contains("sent_status=2");
    }

    @Test
    void aNonZeroStatusOnARequestFrameFailsButAResponseStatusIsStillCompared() throws ReflectiveOperationException {
        String request = frameHex("00000010", "0100", "01", "0000000000000001", "0000", "");
        String okResponse = frameHex("00000010", "0100", "00", "0000000000000002", "0000", "");

        assertThat(sentOnce(true, request)).singleElement().asString().endsWith(" sent_status=1");
        // 클라이언트 쪽에서 짝수 stream_id는 응답이다. status는 기존대로 기대와 비교한다.
        assertThat(sentOnce(true, okResponse))
                .containsExactly(expectedOf("{\"type\":256,\"stream_id\":\"2\",\"status\":\"OK\"}"));
    }

    private static final String CLOSE_PAYLOAD_NO_ERROR = "00000000" + "0000000000000000";

    private static List<String> closeWithReason(String reasonHex, String expectedSend)
            throws ReflectiveOperationException {
        int length = 16 + 12 + reasonHex.length() / 2;
        String hex = frameHex(
                String.format("%08x", length),
                "0003",
                "00",
                "0000000000000000",
                "0000",
                CLOSE_PAYLOAD_NO_ERROR + reasonHex);
        VectorRunner runner = new VectorRunner(true, false, Endpoint.client(EndpointConfig.defaults()));
        return FrameKind.comparable(
                runner.describeMessages(Arrays.asList(binaryMessage(Bytes.fromHex(hex))), false),
                JsonParser.parseString(expectedSend).getAsJsonArray());
    }

    private static String expectedClose(String reasonField) {
        return "[{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":\"0\"" + reasonField + "}]";
    }

    private static List<String> expectedCloseRendered(String reasonField) {
        List<String> result = new ArrayList<>();
        for (com.google.gson.JsonElement element :
                JsonParser.parseString(expectedClose(reasonField)).getAsJsonArray()) {
            result.add(FrameKind.expected(element.getAsJsonObject()));
        }
        return result;
    }

    @Test
    void aCloseReasonIsNotComparedWhenTheVectorOmitsIt() throws ReflectiveOperationException {
        assertThat(closeWithReason("616263", expectedClose(""))).isEqualTo(expectedCloseRendered(""));
        assertThat(closeWithReason("", expectedClose(""))).isEqualTo(expectedCloseRendered(""));
    }

    @Test
    void aCloseReasonTheVectorListsIsComparedByTheByte() throws ReflectiveOperationException {
        String listed = ",\"reason\":\"abc\"";
        assertThat(closeWithReason("616263", expectedClose(listed))).isEqualTo(expectedCloseRendered(listed));
        assertThat(closeWithReason("616264", expectedClose(listed))).isNotEqualTo(expectedCloseRendered(listed));
        assertThat(closeWithReason("", expectedClose(listed))).isNotEqualTo(expectedCloseRendered(listed));
    }

    @Test
    void anEmptyListedCloseReasonFailsWhenTheEndpointExplained() throws ReflectiveOperationException {
        String empty = ",\"reason\":\"\"";
        assertThat(closeWithReason("", expectedClose(empty))).isEqualTo(expectedCloseRendered(empty));
        assertThat(closeWithReason("616263", expectedClose(empty))).isNotEqualTo(expectedCloseRendered(empty));
    }

    @Test
    void theListedReasonFollowsItsEntryEvenAfterOtherFramesAndMessages() throws ReflectiveOperationException {
        String ping = frameHex("00000018", "0002", "00", "0000000000000000", "0000", "0102030405060708");
        String close =
                frameHex("0000001f", "0003", "00", "0000000000000000", "0000", CLOSE_PAYLOAD_NO_ERROR + "616263");
        VectorRunner runner = new VectorRunner(true, true, Endpoint.client(EndpointConfig.defaults()));
        List<OutboundMessage> messages =
                Arrays.asList(binaryMessage(Bytes.fromHex(ping)), binaryMessage(Bytes.fromHex(close)));
        String expected = "[" + PING_EXPECTED + ","
                + "{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":\"0\",\"reason\":\"abc\"}]";

        List<String> actual = FrameKind.comparable(
                runner.describeMessages(messages, false),
                JsonParser.parseString(expected).getAsJsonArray());

        assertThat(actual.get(0)).doesNotContain("reason");
        assertThat(actual.get(1)).endsWith("reason=616263");
    }

    @Test
    void aServerRunnerTreatsItsOwnEvenStreamAsARequestAndOddAsAResponse() throws ReflectiveOperationException {
        String ownRequest = frameHex("00000010", "0100", "01", "0000000000000002", "0000", "");
        String peerResponse = frameHex("00000010", "0100", "00", "0000000000000001", "0000", "");

        assertThat(sentOnce(false, ownRequest)).singleElement().asString().endsWith(" sent_status=1");
        assertThat(sentOnce(false, peerResponse))
                .containsExactly(expectedOf("{\"type\":256,\"stream_id\":\"1\",\"status\":\"OK\"}"));
    }
}
