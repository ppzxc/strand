package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/** 벡터 형식 문서 형식 벡터 파일 로더의 공개 인터페이스({@link VectorLoader#parse})를 검증한다. */
class VectorLoaderTest {

    private static final String SETUP = "{\"role\":\"server\",\"established\":true}";
    private static final String STEP = "{\"kind\":\"recv\",\"hex\":\"00ff\",\"send\":[],\"events\":[]}";
    private static final String VALID_VECTOR = vector("a", "[\"3.4\"]", SETUP, "[" + STEP + "]");

    private static String vector(String id, String spec, String setup, String steps) {
        return "{\"id\":\"" + id + "\",\"description\":\"d\",\"spec\":" + spec + ",\"setup\":" + setup + ",\"steps\":"
                + steps + "}";
    }

    private static String file(String... vectors) {
        return "{\"format\":2,\"vectors\":[" + String.join(",", vectors) + "]}";
    }

    /** 유효한 벡터 하나에 단계 하나만 바꿔 넣은 파일. */
    private static String fileWithStep(String stepFields) {
        return file(vector("a", "[\"3.4\"]", SETUP, "[{" + stepFields + "}]"));
    }

    private static String fileWithSetup(String setup) {
        return file(vector("a", "[\"3.4\"]", setup, "[" + STEP + "]"));
    }

    private static String recvWithSend(String frame) {
        return fileWithStep("\"kind\":\"recv\",\"hex\":\"00\",\"send\":[" + frame + "],\"events\":[]");
    }

    private static String recvWithEvent(String event) {
        return fileWithStep("\"kind\":\"recv\",\"hex\":\"00\",\"send\":[],\"events\":[" + event + "]");
    }

    @Test
    void rejectsResultEventWithoutCertainty() {
        assertThatThrownBy(() -> VectorLoader.parse(
                        "x.json", recvWithEvent("{\"event\":\"result\",\"stream_id\":\"1\",\"status\":\"OK\"}")))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContainingAll("필수 필드 누락", "certainty");
    }

    @Test
    void rejectsResultEventWithUnknownCertainty() {
        assertThatThrownBy(
                        () -> VectorLoader.parse(
                                "x.json",
                                recvWithEvent(
                                        "{\"event\":\"result\",\"stream_id\":\"1\",\"status\":\"OK\",\"certainty\":\"maybe\"}")))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContainingAll("certainty", "maybe");
    }

    @Test
    void rejectsAResultWithoutStreamIdThatIsNotAnUnavailableNotProcessed() {
        assertThatThrownBy(() -> VectorLoader.parse(
                        "x.json",
                        recvWithEvent("{\"event\":\"result\",\"status\":\"OK\",\"certainty\":\"not_processed\"}")))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContainingAll("status", "UNAVAILABLE");
        assertThatThrownBy(() -> VectorLoader.parse(
                        "x.json",
                        recvWithEvent("{\"event\":\"result\",\"status\":\"UNAVAILABLE\",\"certainty\":\"unknown\"}")))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContainingAll("certainty", "not_processed");
    }

    @Test
    void acceptsEveryCertaintyValueAndAResultWithoutStreamId() {
        for (String certainty : new String[] {"response", "not_processed", "unknown"}) {
            assertThat(VectorLoader.parse(
                            "x.json",
                            recvWithEvent("{\"event\":\"result\",\"stream_id\":\"1\",\"status\":\"OK\",\"certainty\":\""
                                    + certainty + "\"}")))
                    .hasSize(1);
        }
        assertThat(VectorLoader.parse(
                        "x.json",
                        recvWithEvent(
                                "{\"event\":\"result\",\"status\":\"UNAVAILABLE\",\"certainty\":\"not_processed\"}")))
                .hasSize(1);
    }

    /** 벡터 형식 6.2절: 번호를 받지 못한 대기열 요청의 CANCELLED와 DEADLINE_EXCEEDED도 stream_id 없는 result다(규격 4.4절, 4.4.1절). */
    @Test
    void acceptsQueuedRequestOutcomesWithoutStreamId() {
        for (String status : new String[] {"UNAVAILABLE", "CANCELLED", "DEADLINE_EXCEEDED"}) {
            assertThat(VectorLoader.parse(
                            "x.json",
                            recvWithEvent("{\"event\":\"result\",\"status\":\"" + status
                                    + "\",\"certainty\":\"not_processed\"}")))
                    .as(status)
                    .hasSize(1);
            assertThatThrownBy(() -> VectorLoader.parse(
                            "x.json",
                            recvWithEvent(
                                    "{\"event\":\"result\",\"status\":\"" + status + "\",\"certainty\":\"unknown\"}")))
                    .as(status)
                    .isInstanceOf(InvalidVectorException.class)
                    .hasMessageContainingAll("certainty", "not_processed");
        }
    }

    @Test
    void requestStepMayCarryAPositiveDeadline() {
        String step = "\"kind\":\"request\",\"type\":257,\"deadline_ms\":100,\"send\":[],\"events\":[]";
        assertThat(VectorLoader.parse("x.json", fileWithStep(step))).hasSize(1);
        assertThatThrownBy(() -> VectorLoader.parse("x.json", fileWithStep(step.replace("100", "0"))))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContaining("deadline_ms");
    }

    @Test
    void loadsAppendixGExample() {
        String json = "{\"format\":2,\"vectors\":[{\"id\":\"ping-is-acknowledged\","
                + "\"description\":\"PING을 받으면 같은 8바이트에 ACK를 붙여 돌려준다\",\"spec\":[\"3.4\"],"
                + "\"setup\":{\"role\":\"server\",\"established\":true},\"steps\":[{\"kind\":\"recv\","
                + "\"hex\":\"00000014 00020000 00000000 00000000 0102030405060708\","
                + "\"send\":[{\"type\":\"PING\",\"flags\":[\"ACK\"],\"payload\":\"0102030405060708\"}],"
                + "\"events\":[]}]}]}";

        List<ConformanceVector> vectors = VectorLoader.parse("x.json", json);

        assertThat(vectors).hasSize(1);
        ConformanceVector vector = vectors.get(0);
        assertThat(vector.id()).isEqualTo("ping-is-acknowledged");
        assertThat(vector.description()).contains("PING");
        assertThat(vector.spec()).containsExactly("3.4");
        assertThat(vector.steps()).hasSize(1);
    }

    @Test
    void optionalFieldsMayBeOmitted() {
        // setup의 local_hello/peer_hello/config, request의 extensions/payload, 프레임의 flags/params 등을 생략한다.
        String steps = "[" + STEP
                + ",{\"kind\":\"request\",\"type\":256,\"send\":[{\"type\":\"PREFACE\"},{\"type\":\"HELLO\"},"
                + "{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":\"0\"},"
                + "{\"type\":\"RESET\",\"stream_id\":\"1\",\"error_code\":\"CANCEL\"},"
                + "{\"type\":256,\"stream_id\":\"1\"}],\"events\":[]},"
                + "{\"kind\":\"respond\",\"stream_id\":\"1\",\"status\":\"OK\",\"send\":[],\"events\":[]}]";

        assertThat(VectorLoader.parse("x.json", file(vector("a", "[\"3.4\"]", SETUP, steps))))
                .hasSize(1);
    }

    @Test
    void acceptsOptionalFieldsWhenPresent() {
        String setup = "{\"role\":\"client\",\"established\":true,"
                + "\"local_hello\":{\"max_frame_size\":16384,\"max_concurrent_streams\":1,\"capabilities\":0},"
                + "\"peer_hello\":{},\"config\":{\"ack_timeout_ms\":5000},"
                + "\"transport\":\"tcp\",\"next_stream_id\":\"1\",\"known_types\":[256,257]}";
        String steps = "["
                + "{\"kind\":\"request\",\"type\":257,\"extensions\":[{\"tag\":1,\"value\":\"0a\"}],\"payload\":\"0b\","
                + "\"send\":[{\"type\":\"PREFACE\"},{\"type\":257,\"stream_id\":\"1\","
                + "\"status\":17,\"extensions\":[{\"tag\":1,\"value\":\"\"}],\"payload\":\"\"},"
                + "{\"type\":\"HELLO\",\"flags\":[\"ACK\"],\"stream_id\":\"0\",\"params\":{\"max_frame_size\":1}},"
                + "{\"type\":\"CLOSE\",\"stream_id\":\"0\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":\"0\"},"
                + "{\"type\":\"RESET\",\"stream_id\":\"1\",\"error_code\":8}],"
                + "\"events\":[{\"event\":\"request_received\",\"stream_id\":\"1\",\"type\":257},"
                + "{\"event\":\"result\",\"stream_id\":\"1\",\"status\":\"OK\",\"certainty\":\"response\"},{\"event\":\"closed\"}]},"
                + "{\"kind\":\"respond\",\"stream_id\":\"9007199254740993\",\"status\":\"INTERNAL\",\"send\":[],\"events\":[]},"
                + "{\"kind\":\"cancel\",\"stream_id\":\"1\",\"send\":[],\"events\":[]},"
                + "{\"kind\":\"close\",\"error_code\":\"NO_ERROR\",\"send\":[],\"events\":[]},"
                + "{\"kind\":\"advance\",\"ms\":1,\"send\":[],\"events\":[]},"
                + "{\"kind\":\"ping\",\"payload\":\"0102030405060708\",\"send\":[],\"events\":[]},"
                + "{\"kind\":\"disconnect\",\"send\":[],\"events\":[{\"event\":\"closed\"}]}]";

        assertThat(VectorLoader.parse("x.json", file(vector("a", "[\"2.4.2\",\"5.1\"]", setup, steps))))
                .hasSize(1);
    }

    @Test
    void streamNumbersAboveTwoToThe53AreReadExactly() {
        String big = "9007199254740993"; // 2^53 + 1: double로는 표현할 수 없다
        String setup = "{\"role\":\"client\",\"established\":true,\"next_stream_id\":\"" + big + "\"}";
        String steps = "[{\"kind\":\"respond\",\"stream_id\":\"" + big + "\",\"status\":\"OK\","
                + "\"send\":[{\"type\":\"RESET\",\"stream_id\":\"" + big + "\",\"error_code\":\"CANCEL\"}],"
                + "\"events\":[{\"event\":\"result\",\"stream_id\":\"" + big
                + "\",\"status\":\"OK\",\"certainty\":\"response\"}]}]";

        ConformanceVector vector = VectorLoader.parse("x.json", file(vector("a", "[\"3.4\"]", setup, steps)))
                .get(0);

        assertThat(vector.setup().get("next_stream_id").getAsString()).isEqualTo(big);
        assertThat(vector.steps().get(0).get("stream_id").getAsString()).isEqualTo(big);
        assertThat(vector.steps()
                        .get(0)
                        .getAsJsonArray("send")
                        .get(0)
                        .getAsJsonObject()
                        .get("stream_id")
                        .getAsString())
                .isEqualTo(big);
        assertThat(vector.steps()
                        .get(0)
                        .getAsJsonArray("events")
                        .get(0)
                        .getAsJsonObject()
                        .get("stream_id")
                        .getAsString())
                .isEqualTo(big);
    }

    @TestFactory
    Stream<DynamicTest> rejectsNumericStreamNumbers() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected(
                "step stream_id",
                fileWithStep("\"kind\":\"cancel\",\"stream_id\":1,\"send\":[],\"events\":[]"),
                "stream_id",
                "문자열"));
        t.add(rejected("app frame stream_id", recvWithSend("{\"type\":256,\"stream_id\":1}"), "stream_id", "문자열"));
        t.add(rejected(
                "reset stream_id",
                recvWithSend("{\"type\":\"RESET\",\"stream_id\":1,\"error_code\":\"CANCEL\"}"),
                "stream_id",
                "문자열"));
        t.add(rejected(
                "control frame stream_id", recvWithSend("{\"type\":\"HELLO\",\"stream_id\":0}"), "stream_id", "문자열"));
        t.add(rejected(
                "event stream_id",
                recvWithEvent("{\"event\":\"result\",\"stream_id\":1,\"status\":\"OK\",\"certainty\":\"response\"}"),
                "stream_id",
                "문자열"));
        t.add(rejected(
                "on RESET",
                recvWithSend(
                        "{\"type\":\"RESET\",\"stream_id\":\"1\",\"error_code\":\"CANCEL\",\"last_stream_id\":\"0\"}"),
                "알 수 없는 필드",
                "last_stream_id"));
        t.add(rejected(
                "leading zero",
                fileWithStep("\"kind\":\"cancel\",\"stream_id\":\"01\",\"send\":[],\"events\":[]"),
                "stream_id"));
        t.add(rejected(
                "signed",
                fileWithStep("\"kind\":\"cancel\",\"stream_id\":\"+1\",\"send\":[],\"events\":[]"),
                "stream_id"));
        return t.stream();
    }

    private static DynamicTest rejected(String name, String json, String... messageParts) {
        return DynamicTest.dynamicTest(
                name,
                () -> assertThatThrownBy(() -> VectorLoader.parse("x.json", json))
                        .isInstanceOf(InvalidVectorException.class)
                        .hasMessageContaining("x.json")
                        .hasMessageContainingAll(messageParts));
    }

    @TestFactory
    Stream<DynamicTest> rejectsTopLevelViolations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected("higher format", "{\"format\":3,\"vectors\":[]}", "format"));
        t.add(rejected("format 1", "{\"format\":1,\"vectors\":[]}", "format"));
        t.add(rejected("zero format", "{\"format\":0,\"vectors\":[]}", "format"));
        t.add(rejected("non-integer format", "{\"format\":\"2\",\"vectors\":[]}", "format"));
        t.add(rejected("missing format", "{\"vectors\":[]}", "필수 필드 누락", "format"));
        t.add(rejected("missing vectors", "{\"format\":2}", "필수 필드 누락", "vectors"));
        t.add(rejected("unknown top-level field", "{\"format\":2,\"vectors\":[],\"x\":1}", "알 수 없는 필드", "x"));
        t.add(rejected("legacy top-level array", "[]", "객체"));
        t.add(rejected("empty file", "  \n", "비어"));
        t.add(rejected("syntax error", "{\"format\":", "JSON"));
        t.add(rejected("trailing content", "{\"format\":2,\"vectors\":[]} []", "JSON"));
        t.add(rejected("duplicate json key", "{\"format\":2,\"format\":2,\"vectors\":[]}", "중복 키", "format"));
        t.add(rejected("vector not an object", "{\"format\":2,\"vectors\":[\"a\"]}", "[0]", "객체"));
        return t.stream();
    }

    @TestFactory
    Stream<DynamicTest> rejectsVectorViolations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected(
                "unknown vector field",
                file(VALID_VECTOR.replace("\"id\":\"a\"", "\"id\":\"a\",\"result\":\"valid\"")),
                "알 수 없는 필드",
                "result",
                "a"));
        t.add(rejected("missing id", file(VALID_VECTOR.replace("\"id\":\"a\",", "")), "필수 필드 누락", "id"));
        t.add(rejected(
                "missing description",
                file(VALID_VECTOR.replace("\"description\":\"d\",", "")),
                "필수 필드 누락",
                "description",
                "a"));
        t.add(rejected("missing spec", file(VALID_VECTOR.replace("\"spec\":[\"3.4\"],", "")), "필수 필드 누락", "spec", "a"));
        t.add(rejected(
                "missing setup", file(VALID_VECTOR.replace("\"setup\":" + SETUP + ",", "")), "필수 필드 누락", "setup", "a"));
        t.add(rejected(
                "missing steps",
                file(vector("a", "[\"3.4\"]", SETUP, "[]").replace(",\"steps\":[]", "")),
                "필수 필드 누락",
                "steps",
                "a"));
        t.add(rejected("duplicate id in file", file(VALID_VECTOR, VALID_VECTOR), "id 중복", "a"));
        for (String badId : new String[] {"", "A", "a_b", "-a", "a-", "a--b", "a b", "가"}) {
            t.add(rejected("bad id '" + badId + "'", file(vector(badId, "[\"3.4\"]", SETUP, "[" + STEP + "]")), "id"));
        }
        t.add(rejected("non-string id", file(VALID_VECTOR.replace("\"id\":\"a\"", "\"id\":1")), "id", "문자열"));
        t.add(rejected(
                "empty description",
                file(VALID_VECTOR.replace("\"description\":\"d\"", "\"description\":\"\"")),
                "description",
                "a"));
        t.add(rejected("empty spec", file(vector("a", "[]", SETUP, "[" + STEP + "]")), "spec", "a"));
        t.add(rejected(
                "spec element not a section number",
                file(vector("a", "[\"#12\"]", SETUP, "[" + STEP + "]")),
                "spec",
                "a"));
        t.add(rejected("spec element not string", file(vector("a", "[3]", SETUP, "[" + STEP + "]")), "spec", "a"));
        t.add(rejected("empty steps", file(vector("a", "[\"3.4\"]", SETUP, "[]")), "steps", "a"));
        return t.stream();
    }

    @TestFactory
    Stream<DynamicTest> rejectsSetupViolations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected("missing role", fileWithSetup("{\"established\":true}"), "role", "a"));
        t.add(rejected("missing established", fileWithSetup("{\"role\":\"client\"}"), "established", "a"));
        t.add(rejected("unknown field", fileWithSetup("{\"role\":\"client\",\"established\":true,\"x\":1}"), "x", "a"));
        t.add(rejected("bad role", fileWithSetup("{\"role\":\"peer\",\"established\":true}"), "role", "a"));
        t.add(rejected(
                "unknown hello param",
                fileWithSetup("{\"role\":\"client\",\"established\":true,\"local_hello\":{\"x\":1}}"),
                "x",
                "a"));
        t.add(rejected(
                "unknown config item",
                fileWithSetup("{\"role\":\"client\",\"established\":true,\"config\":{\"x\":1}}"),
                "x",
                "a"));
        t.add(rejected(
                "config out of range",
                fileWithSetup("{\"role\":\"client\",\"established\":true,\"config\":{\"ack_timeout_ms\":0}}"),
                "ack_timeout_ms",
                "a"));
        return t.stream();
    }

    @TestFactory
    Stream<DynamicTest> rejectsStepViolations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected(
                "unknown step kind", fileWithStep("\"kind\":\"wait\",\"send\":[],\"events\":[]"), "kind", "wait", "a"));
        t.add(rejected("missing step kind", fileWithStep("\"send\":[],\"events\":[]"), "kind", "a"));
        t.add(rejected("missing send", fileWithStep("\"kind\":\"recv\",\"hex\":\"00\",\"events\":[]"), "send", "a"));
        t.add(rejected("missing events", fileWithStep("\"kind\":\"recv\",\"hex\":\"00\",\"send\":[]"), "events", "a"));
        t.add(rejected("recv missing hex", fileWithStep("\"kind\":\"recv\",\"send\":[],\"events\":[]"), "hex", "a"));
        t.add(rejected(
                "unknown step field",
                fileWithStep("\"kind\":\"recv\",\"hex\":\"00\",\"send\":[],\"events\":[],\"x\":1"),
                "x",
                "a"));
        t.add(rejected(
                "field of another kind",
                fileWithStep("\"kind\":\"recv\",\"hex\":\"00\",\"ms\":1,\"send\":[],\"events\":[]"),
                "ms",
                "a"));
        t.add(rejected(
                "request type below 256",
                fileWithStep("\"kind\":\"request\",\"type\":255,\"send\":[],\"events\":[]"),
                "type",
                "a"));
        t.add(rejected(
                "respond bad status",
                fileWithStep("\"kind\":\"respond\",\"stream_id\":\"1\",\"status\":\"NOPE\",\"send\":[],\"events\":[]"),
                "status",
                "a"));
        t.add(rejected(
                "respond reserved status below 17",
                fileWithStep("\"kind\":\"respond\",\"stream_id\":\"1\",\"status\":16,\"send\":[],\"events\":[]"),
                "status",
                "a"));
        t.add(rejected(
                "close bad error code",
                fileWithStep("\"kind\":\"close\",\"error_code\":\"NOPE\",\"send\":[],\"events\":[]"),
                "error_code",
                "a"));
        t.add(rejected(
                "advance zero", fileWithStep("\"kind\":\"advance\",\"ms\":0,\"send\":[],\"events\":[]"), "ms", "a"));
        t.add(rejected(
                "disconnect unknown field",
                fileWithStep("\"kind\":\"disconnect\",\"ms\":1,\"send\":[],\"events\":[]"),
                "알 수 없는 필드",
                "ms"));
        t.add(rejected(
                "disconnect missing events",
                fileWithStep("\"kind\":\"disconnect\",\"send\":[]"),
                "필수 필드 누락",
                "events"));
        t.add(rejected(
                "stream_id above 2^63-1",
                fileWithStep("\"kind\":\"cancel\",\"stream_id\":\"9223372036854775808\",\"send\":[],\"events\":[]"),
                "stream_id",
                "a"));
        t.add(rejected(
                "send not an array",
                fileWithStep("\"kind\":\"recv\",\"hex\":\"00\",\"send\":{},\"events\":[]"),
                "send",
                "a"));
        return t.stream();
    }

    @TestFactory
    Stream<DynamicTest> rejectsBadHex() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected(
                "odd digits", fileWithStep("\"kind\":\"recv\",\"hex\":\"00f\",\"send\":[],\"events\":[]"), "홀수", "a"));
        t.add(rejected(
                "non-hex char",
                fileWithStep("\"kind\":\"recv\",\"hex\":\"00zz\",\"send\":[],\"events\":[]"),
                "hex",
                "a"));
        t.add(rejected(
                "uppercase", fileWithStep("\"kind\":\"recv\",\"hex\":\"00FF\",\"send\":[],\"events\":[]"), "대문자", "a"));
        t.add(rejected(
                "odd digits in request payload",
                fileWithStep("\"kind\":\"request\",\"type\":256,\"payload\":\"0\",\"send\":[],\"events\":[]"),
                "홀수",
                "a"));
        t.add(rejected(
                "uppercase in extension value",
                fileWithStep(
                        "\"kind\":\"request\",\"type\":256,\"extensions\":[{\"tag\":1,\"value\":\"0A\"}],\"send\":[],\"events\":[]"),
                "대문자",
                "a"));
        t.add(rejected(
                "extension missing value",
                fileWithStep(
                        "\"kind\":\"request\",\"type\":256,\"extensions\":[{\"tag\":1}],\"send\":[],\"events\":[]"),
                "value",
                "a"));
        return t.stream();
    }

    @TestFactory
    Stream<DynamicTest> rejectsBadExpectations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected("unknown frame type name", recvWithSend("{\"type\":\"PONG\"}"), "type", "a"));
        t.add(rejected("app frame type below 256", recvWithSend("{\"type\":3,\"stream_id\":\"1\"}"), "type", "a"));
        t.add(rejected("ping missing payload", recvWithSend("{\"type\":\"PING\"}"), "payload", "a"));
        t.add(rejected("ping short payload", recvWithSend("{\"type\":\"PING\",\"payload\":\"01\"}"), "payload", "a"));
        t.add(rejected(
                "reset missing stream_id",
                recvWithSend("{\"type\":\"RESET\",\"error_code\":\"NO_ERROR\"}"),
                "stream_id",
                "a"));
        t.add(rejected("frame unknown field", recvWithSend("{\"type\":\"PREFACE\",\"x\":1}"), "x", "a"));
        t.add(rejected("unknown event", recvWithEvent("{\"event\":\"opened\"}"), "event", "a"));
        t.add(rejected(
                "result event missing status",
                recvWithEvent("{\"event\":\"result\",\"stream_id\":\"1\",\"certainty\":\"response\"}"),
                "status",
                "a"));
        t.add(rejected("closed event unknown field", recvWithEvent("{\"event\":\"closed\",\"x\":1}"), "x", "a"));
        return t.stream();
    }

    private static void assertMessage(String json, String expected) {
        assertThatThrownBy(() -> VectorLoader.parse("x.json", json))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessage(expected);
    }

    /** 오류 메시지의 {@code 파일명: [위치](id=...) 경로: 메시지} 형식을 고정한다. */
    @Test
    void errorMessagesFollowFileLocationPathFormat() {
        assertMessage("{\"format\":1,\"vectors\":[]}", "x.json: format: 지원하지 않는 format이다(아는 것은 2): 1");
        assertThatThrownBy(() -> VectorLoader.parse("x.json", "{\"format\":2,\"vectors\":[]"))
                .hasMessageStartingWith("x.json: JSON 문법 오류: ");
        assertMessage("  ", "x.json: 파일이 비어 있다");
        assertMessage("[]", "x.json: 객체여야 한다");
        assertMessage("{\"format\":2,\"vectors\":[1]}", "x.json: [0] 객체여야 한다");
        assertMessage(
                file(VALID_VECTOR.replace("\"description\":\"d\"", "\"description\":\"\"")),
                "x.json: [0](id=a) description: 비어 있으면 안 된다");
        assertMessage(file(VALID_VECTOR, VALID_VECTOR), "x.json: [1](id=a) id 중복");
        assertMessage(
                recvWithSend("{\"type\":\"PING\",\"payload\":\"01\"}"),
                "x.json: [0](id=a) steps[0].send[0].payload: 공백 없는 소문자 16자리 hex여야 한다: '01'");
        assertMessage(
                fileWithStep("\"kind\":\"wait\",\"send\":[],\"events\":[]"),
                "x.json: [0](id=a) steps[0].kind: 알 수 없는 단계 kind: wait");
        assertMessage(
                recvWithEvent("{\"event\":\"opened\"}"),
                "x.json: [0](id=a) steps[0].events[0].event: 알 수 없는 event: opened");
        assertMessage(
                recvWithSend("{\"type\":\"PONG\"}"), "x.json: [0](id=a) steps[0].send[0].type: 알 수 없는 프레임 type: PONG");
        assertMessage(recvWithSend("{}"), "x.json: [0](id=a) steps[0].send[0]: 필수 필드 누락: type");
        assertMessage(recvWithEvent("{}"), "x.json: [0](id=a) steps[0].events[0]: 필수 필드 누락: event");
        assertMessage(fileWithStep("\"send\":[],\"events\":[]"), "x.json: [0](id=a) steps[0]: 필수 필드 누락: kind");
    }

    @TestFactory
    Stream<DynamicTest> rejectsControlFrameFieldViolations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected("hello flag other than ACK", recvWithSend("{\"type\":\"HELLO\",\"flags\":[\"END\"]}"), "flags"));
        t.add(rejected(
                "hello stream_id not zero", recvWithSend("{\"type\":\"HELLO\",\"stream_id\":\"1\"}"), "stream_id"));
        t.add(rejected(
                "ping duplicate flag",
                recvWithSend("{\"type\":\"PING\",\"flags\":[\"ACK\",\"ACK\"],\"payload\":\"0102030405060708\"}"),
                "플래그 중복"));
        t.add(rejected(
                "ping stream_id not zero",
                recvWithSend("{\"type\":\"PING\",\"stream_id\":\"2\",\"payload\":\"0102030405060708\"}"),
                "stream_id"));
        t.add(rejected(
                "ping payload uppercase",
                recvWithSend("{\"type\":\"PING\",\"payload\":\"010203040506070A\"}"),
                "payload"));
        t.add(rejected(
                "ping payload with spaces",
                recvWithSend("{\"type\":\"PING\",\"payload\":\"01020304 05060708\"}"),
                "payload"));
        t.add(rejected(
                "close stream_id not zero",
                recvWithSend(
                        "{\"type\":\"CLOSE\",\"stream_id\":\"1\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":\"0\"}"),
                "stream_id"));
        t.add(rejected(
                "reset stream_id zero",
                recvWithSend("{\"type\":\"RESET\",\"stream_id\":\"0\",\"error_code\":\"CANCEL\"}"),
                "stream_id"));
        t.add(rejected("app frame missing stream_id", recvWithSend("{\"type\":256}"), "필수 필드 누락", "stream_id"));
        t.add(rejected("hello params unknown field", recvWithSend("{\"type\":\"HELLO\",\"params\":{\"x\":1}}"), "x"));
        t.add(rejected(
                "hello params out of u32",
                recvWithSend("{\"type\":\"HELLO\",\"params\":{\"max_frame_size\":4294967296}}"),
                "max_frame_size"));
        t.add(rejected(
                "extension tag out of u16",
                fileWithStep(
                        "\"kind\":\"request\",\"type\":256,\"extensions\":[{\"tag\":65536,\"value\":\"\"}],\"send\":[],\"events\":[]"),
                "tag"));
        return t.stream();
    }

    /** 이름 대신 쓰는 정수의 예약 값 범위: status는 17~255, error_code는 8~2^32-1. */
    @Test
    void namedCodesAcceptOnlyTheReservedIntegerRange() {
        assertThat(parseStatus(17)).hasSize(1);
        assertThat(parseStatus(255)).hasSize(1);
        assertThatThrownBy(() -> parseStatus(16)).isInstanceOf(InvalidVectorException.class);
        assertThatThrownBy(() -> parseStatus(256)).isInstanceOf(InvalidVectorException.class);
        assertThat(parseErrorCode("8")).hasSize(1);
        assertThat(parseErrorCode("4294967295")).hasSize(1);
        assertThatThrownBy(() -> parseErrorCode("7")).isInstanceOf(InvalidVectorException.class);
        assertThatThrownBy(() -> parseErrorCode("4294967296")).isInstanceOf(InvalidVectorException.class);
    }

    private static List<ConformanceVector> parseStatus(int status) {
        return VectorLoader.parse(
                "x.json",
                fileWithStep("\"kind\":\"respond\",\"stream_id\":\"1\",\"status\":" + status
                        + ",\"send\":[],\"events\":[]"));
    }

    private static List<ConformanceVector> parseErrorCode(String code) {
        return VectorLoader.parse(
                "x.json", fileWithStep("\"kind\":\"close\",\"error_code\":" + code + ",\"send\":[],\"events\":[]"));
    }

    private static String setupWith(String extraFields) {
        return fileWithSetup("{\"role\":\"server\",\"established\":true," + extraFields + "}");
    }

    private static String clientSetupWith(String extraFields) {
        return fileWithSetup("{\"role\":\"client\",\"established\":true," + extraFields + "}");
    }

    private static final String RECV_STEP = "{\"kind\":\"recv\",\"hex\":\"00\",\"send\":[],\"events\":[]}";
    private static final String MESSAGE_STEP =
            "{\"kind\":\"recv_message\",\"opcode\":\"binary\",\"hex\":\"00\",\"send\":[],\"events\":[]}";

    private static String fileWithTransportAndStep(String transport, String step) {
        return file(vector(
                "a",
                "[\"3.4\"]",
                "{\"role\":\"server\",\"established\":true,\"transport\":\"" + transport + "\"}",
                "[" + step + "]"));
    }

    @Test
    void closeRequiresLastStreamIdAsDecimalString() {
        assertThat(
                        VectorLoader.parse(
                                "x.json",
                                recvWithSend(
                                        "{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":\"18446744073709551615\"}")))
                .hasSize(1);
        assertThat(VectorLoader.parse(
                        "x.json",
                        recvWithSend("{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":\"0\"}")))
                .hasSize(1);
    }

    @TestFactory
    Stream<DynamicTest> rejectsInvalidCloseLastStreamId() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected(
                "missing",
                recvWithSend("{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\"}"),
                "필수 필드 누락",
                "last_stream_id"));
        t.add(rejected(
                "json number",
                recvWithSend("{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":1}"),
                "last_stream_id",
                "문자열"));
        t.add(rejected(
                "above u64",
                recvWithSend(
                        "{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":\"18446744073709551616\"}"),
                "last_stream_id"));
        t.add(rejected(
                "leading zero",
                recvWithSend("{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":\"01\"}"),
                "last_stream_id"));
        return t.stream();
    }

    @TestFactory
    Stream<DynamicTest> rejectsFieldsRemovedInSpecification010() {
        List<DynamicTest> t = new ArrayList<>();
        for (String hello : new String[] {"max_pending_bytes", "content_types", "content_type"}) {
            t.add(rejected(
                    "local_hello." + hello, setupWith("\"local_hello\":{\"" + hello + "\":1}"), "알 수 없는 필드", hello));
        }
        t.add(rejected(
                "config.draining_retention_limit",
                setupWith("\"config\":{\"draining_retention_limit\":1}"),
                "알 수 없는 필드",
                "draining_retention_limit"));
        t.add(rejected(
                "config.hello_timeout_ms",
                setupWith("\"config\":{\"hello_timeout_ms\":1000}"),
                "알 수 없는 필드",
                "hello_timeout_ms"));
        for (String flag : new String[] {"RESPONSE", "END"}) {
            t.add(rejected(
                    "app frame flag " + flag,
                    recvWithSend("{\"type\":256,\"flags\":[\"" + flag + "\"],\"stream_id\":\"1\"}"),
                    "알 수 없는 필드",
                    "flags"));
        }
        for (String name : new String[] {"FRAME_SIZE_ERROR", "HELLO_TIMEOUT", "STREAM_CLOSED"}) {
            t.add(rejected(
                    "error_code " + name,
                    fileWithStep("\"kind\":\"close\",\"error_code\":\"" + name + "\",\"send\":[],\"events\":[]"),
                    "알 수 없는 이름",
                    name));
        }
        return t.stream();
    }

    @Test
    void acceptsEveryErrorCodeNameOfSection252() {
        for (String name : new String[] {
            "NO_ERROR",
            "PROTOCOL_ERROR",
            "INTERNAL_ERROR",
            "REFUSED",
            "CANCEL",
            "EXCESSIVE_USE",
            "UNRESPONSIVE",
            "UNAUTHENTICATED"
        }) {
            assertThat(VectorLoader.parse(
                            "x.json",
                            fileWithStep(
                                    "\"kind\":\"close\",\"error_code\":\"" + name + "\",\"send\":[],\"events\":[]")))
                    .hasSize(1);
        }
    }

    @TestFactory
    Stream<DynamicTest> rejectsPingStepViolations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected(
                "ping step missing payload",
                fileWithStep("\"kind\":\"ping\",\"send\":[],\"events\":[]"),
                "필수 필드 누락",
                "payload"));
        for (String payload : new String[] {"0102", "010203040506070809", "010203040506070A", "01020304 05060708"}) {
            t.add(rejected(
                    "ping step payload '" + payload + "'",
                    fileWithStep("\"kind\":\"ping\",\"payload\":\"" + payload + "\",\"send\":[],\"events\":[]"),
                    "payload"));
        }
        return t.stream();
    }

    private static final String PING_STEP =
            "{\"kind\":\"ping\",\"payload\":\"0102030405060708\",\"send\":[],\"events\":[]}";
    /** 서버 역할이 AUTH(ACK)를 보내 AUTH 관문이 열리는 단계. */
    private static final String AUTH_ACK_SENT_STEP =
            "{\"kind\":\"recv\",\"hex\":\"00\",\"send\":[{\"type\":\"AUTH\",\"flags\":[\"ACK\"]}],\"events\":[]}";
    /** 클라이언트 역할이 서버의 AUTH(ACK)를 받아 AUTH 관문이 열리는 단계. */
    private static final String AUTH_ACK_RECEIVED_STEP = "{\"kind\":\"recv\","
            + "\"hex\":\"00000010 0005 01 00 0000000000000000 0000 0000\",\"send\":[],\"events\":[]}";
    /** 서버 HELLO에 ACK를 보냈을 뿐인 단계: AUTH 관문은 아직 닫혀 있다. */
    private static final String HELLO_ACK_STEP =
            "{\"kind\":\"recv\",\"hex\":\"00\",\"send\":[{\"type\":\"HELLO\",\"flags\":[\"ACK\"]}],\"events\":[]}";

    private static String fileWithEstablished(boolean established, String... steps) {
        return fileWithRole("server", established, steps);
    }

    private static String fileWithRole(String role, boolean established, String... steps) {
        return file(vector(
                "a",
                "[\"3.4\"]",
                "{\"role\":\"" + role + "\",\"established\":" + established + "}",
                "[" + String.join(",", steps) + "]"));
    }

    /** 벡터 형식 문서 8.1절 3번: AUTH 관문이 열리기 전에는 ping 단계를 쓰지 않는다. */
    @TestFactory
    Stream<DynamicTest> rejectsPingStepBeforeTheAuthGateOpens() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected(
                "ping as first step before establishment", fileWithEstablished(false, PING_STEP), "ping", "AUTH"));
        t.add(rejected(
                "server ping after only the HELLO ACK",
                fileWithEstablished(false, HELLO_ACK_STEP, PING_STEP),
                "steps[1]",
                "AUTH"));
        t.add(rejected(
                "server ping before the AUTH(ACK) step",
                fileWithEstablished(false, RECV_STEP, PING_STEP, AUTH_ACK_SENT_STEP),
                "steps[1]",
                "AUTH"));
        t.add(rejected(
                "client ping before AUTH(ACK) arrives",
                fileWithRole("client", false, RECV_STEP, PING_STEP, AUTH_ACK_RECEIVED_STEP),
                "steps[1]",
                "AUTH"));
        return t.stream();
    }

    @Test
    void acceptsPingStepAfterTheAuthGateOpensOrWhenEstablished() {
        assertThat(VectorLoader.parse("x.json", fileWithEstablished(false, AUTH_ACK_SENT_STEP, PING_STEP)))
                .hasSize(1);
        assertThat(VectorLoader.parse("x.json", fileWithRole("client", false, AUTH_ACK_RECEIVED_STEP, PING_STEP)))
                .hasSize(1);
        assertThat(VectorLoader.parse("x.json", fileWithEstablished(true, PING_STEP)))
                .hasSize(1);
        // 확립 전이라도 ping 단계가 없으면 규칙과 무관하다.
        assertThat(VectorLoader.parse("x.json", fileWithEstablished(false, RECV_STEP)))
                .hasSize(1);
    }

    // ---- 연결 인증(규격 3.3.1절, 벡터 형식 문서 4절·6.1절) ----

    private static String authFrame(String fields) {
        return recvWithSend("{\"type\":\"AUTH\"" + (fields.isEmpty() ? "" : "," + fields) + "}");
    }

    @Test
    void acceptsSetupAuthAndTheAuthTimeout() {
        for (String auth : new String[] {
            "{}",
            "{\"mechanism\":\"PLAIN\"}",
            "{\"credentials\":\"0075736572\"}",
            "{\"mechanism\":\"PLAIN\",\"credentials\":\"00 75 73 65 72 00 70 77\"}",
            "{\"mechanism\":\"X-A_1\",\"credentials\":\"\"}",
            "{\"mechanism\":\"ABCDEFGHIJKLMNOPQRST\"}"
        }) {
            assertThat(VectorLoader.parse("x.json", setupWith("\"auth\":" + auth)))
                    .hasSize(1);
        }
        assertThat(VectorLoader.parse("x.json", setupWith("\"config\":{\"auth_timeout_ms\":1000}")))
                .hasSize(1);
    }

    @TestFactory
    Stream<DynamicTest> rejectsSetupAuthViolations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected("auth not an object", setupWith("\"auth\":\"PLAIN\""), "auth", "객체"));
        t.add(rejected("auth unknown field", setupWith("\"auth\":{\"user\":\"x\"}"), "auth", "알 수 없는 필드", "user"));
        t.add(rejected("empty mechanism", setupWith("\"auth\":{\"mechanism\":\"\"}"), "auth.mechanism"));
        t.add(rejected(
                "lowercase mechanism", setupWith("\"auth\":{\"mechanism\":\"plain\"}"), "auth.mechanism", "plain"));
        t.add(rejected("space in mechanism", setupWith("\"auth\":{\"mechanism\":\"A B\"}"), "auth.mechanism"));
        t.add(rejected(
                "21 character mechanism",
                setupWith("\"auth\":{\"mechanism\":\"ABCDEFGHIJKLMNOPQRSTU\"}"),
                "auth.mechanism",
                "20"));
        t.add(rejected("numeric mechanism", setupWith("\"auth\":{\"mechanism\":1}"), "auth.mechanism"));
        t.add(rejected("odd credentials hex", setupWith("\"auth\":{\"credentials\":\"0\"}"), "auth.credentials", "홀수"));
        t.add(rejected(
                "uppercase credentials hex",
                setupWith("\"auth\":{\"credentials\":\"0A\"}"),
                "auth.credentials",
                "대문자"));
        t.add(rejected("zero auth timeout", setupWith("\"config\":{\"auth_timeout_ms\":0}"), "config.auth_timeout_ms"));
        return t.stream();
    }

    @Test
    void acceptsAuthItemsInSend() {
        for (String frame : new String[] {
            "\"mechanism\":\"PLAIN\"",
            "\"mechanism\":\"PLAIN\",\"credentials\":\"0075\"",
            "\"mechanism\":\"ANONYMOUS\",\"credentials\":\"\",\"stream_id\":\"0\",\"flags\":[]",
            "\"flags\":[\"ACK\"]",
            "\"flags\":[\"ACK\"],\"params\":{}",
            "\"flags\":[\"ACK\"],\"params\":{\"credential_lifetime\":4294967295}"
        }) {
            assertThat(VectorLoader.parse("x.json", authFrame(frame))).as(frame).hasSize(1);
        }
    }

    @TestFactory
    Stream<DynamicTest> rejectsAuthItemViolations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected("AUTH without mechanism", authFrame(""), "mechanism"));
        t.add(rejected("AUTH with credentials only", authFrame("\"credentials\":\"00\""), "mechanism"));
        t.add(rejected("AUTH with params", authFrame("\"mechanism\":\"PLAIN\",\"params\":{}"), "params", "ACK"));
        t.add(rejected(
                "AUTH(ACK) with mechanism",
                authFrame("\"flags\":[\"ACK\"],\"mechanism\":\"PLAIN\""),
                "mechanism",
                "ACK"));
        t.add(rejected(
                "AUTH(ACK) with credentials",
                authFrame("\"flags\":[\"ACK\"],\"credentials\":\"00\""),
                "credentials",
                "ACK"));
        t.add(rejected("lowercase mechanism", authFrame("\"mechanism\":\"plain\""), "mechanism", "plain"));
        t.add(rejected("nonzero stream_id", authFrame("\"mechanism\":\"PLAIN\",\"stream_id\":\"1\""), "stream_id"));
        t.add(rejected(
                "unknown identifier name",
                authFrame("\"flags\":[\"ACK\"],\"params\":{\"lifetime\":1}"),
                "params",
                "알 수 없는 필드",
                "lifetime"));
        t.add(rejected(
                "lifetime above u32",
                authFrame("\"flags\":[\"ACK\"],\"params\":{\"credential_lifetime\":4294967296}"),
                "credential_lifetime"));
        t.add(rejected(
                "lifetime as string",
                authFrame("\"flags\":[\"ACK\"],\"params\":{\"credential_lifetime\":\"1\"}"),
                "credential_lifetime"));
        return t.stream();
    }

    @TestFactory
    Stream<DynamicTest> rejectsTransportViolations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected("unknown transport", setupWith("\"transport\":\"udp\""), "transport"));
        t.add(rejected(
                "recv step on websocket", fileWithTransportAndStep("websocket", RECV_STEP), "websocket", "recv"));
        t.add(rejected(
                "recv_message step on explicit tcp",
                fileWithTransportAndStep("tcp", MESSAGE_STEP),
                "tcp",
                "recv_message"));
        t.add(rejected(
                "recv_message step on default transport",
                file(vector("a", "[\"3.4\"]", SETUP, "[" + MESSAGE_STEP + "]")),
                "tcp",
                "recv_message"));
        t.add(rejected(
                "recv_message bad opcode",
                fileWithTransportAndStep("websocket", MESSAGE_STEP.replace("\"binary\"", "\"ping\"")),
                "opcode"));
        t.add(rejected(
                "recv_message missing opcode",
                fileWithTransportAndStep("websocket", MESSAGE_STEP.replace("\"opcode\":\"binary\",", "")),
                "필수 필드 누락",
                "opcode"));
        t.add(rejected(
                "recv_message odd hex",
                fileWithTransportAndStep("websocket", MESSAGE_STEP.replace("\"00\"", "\"0\"")),
                "홀수"));
        return t.stream();
    }

    private static String disconnectWithSend(String send) {
        return "{\"kind\":\"disconnect\",\"send\":[" + send + "],\"events\":[]}";
    }

    @TestFactory
    Stream<DynamicTest> checksWebSocketCloseItems() {
        String close = "{\"type\":\"WEBSOCKET_CLOSE\",\"code\":1000}";
        List<DynamicTest> t = new ArrayList<>();
        t.add(DynamicTest.dynamicTest(
                "accepted on websocket",
                () -> assertThat(VectorLoader.parse(
                                "x.json", fileWithTransportAndStep("websocket", disconnectWithSend(close))))
                        .hasSize(1)));
        t.add(rejected("on tcp", fileWithTransportAndStep("tcp", disconnectWithSend(close)), "tcp", "WEBSOCKET_CLOSE"));
        t.add(rejected(
                "code other than 1000",
                fileWithTransportAndStep("websocket", disconnectWithSend(close.replace("1000", "1001"))),
                "code"));
        t.add(rejected(
                "missing code",
                fileWithTransportAndStep("websocket", disconnectWithSend("{\"type\":\"WEBSOCKET_CLOSE\"}")),
                "필수 필드 누락",
                "code"));
        return t.stream();
    }

    @Test
    void acceptsWebsocketMessagesAndTcpBytes() {
        String text = MESSAGE_STEP.replace("binary", "text");
        assertThat(VectorLoader.parse("x.json", fileWithTransportAndStep("websocket", MESSAGE_STEP)))
                .hasSize(1);
        assertThat(VectorLoader.parse("x.json", fileWithTransportAndStep("websocket", text)))
                .hasSize(1);
        assertThat(VectorLoader.parse("x.json", fileWithTransportAndStep("tcp", RECV_STEP)))
                .hasSize(1);
        assertThat(VectorLoader.parse(
                        "x.json",
                        fileWithTransportAndStep("websocket", "{\"kind\":\"disconnect\",\"send\":[],\"events\":[]}")))
                .hasSize(1);
    }

    @TestFactory
    Stream<DynamicTest> rejectsNextStreamIdViolations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected("zero", clientSetupWith("\"next_stream_id\":\"0\""), "next_stream_id"));
        t.add(rejected("2^63", setupWith("\"next_stream_id\":\"9223372036854775808\""), "next_stream_id"));
        t.add(rejected("number instead of string", clientSetupWith("\"next_stream_id\":1"), "next_stream_id", "문자열"));
        t.add(rejected("not decimal", clientSetupWith("\"next_stream_id\":\"0x1\""), "next_stream_id"));
        t.add(rejected("leading zero", clientSetupWith("\"next_stream_id\":\"01\""), "next_stream_id"));
        t.add(rejected(
                "above 2^63-1", clientSetupWith("\"next_stream_id\":\"9223372036854775809\""), "next_stream_id"));
        t.add(rejected(
                "even for client", clientSetupWith("\"next_stream_id\":\"2\""), "next_stream_id", "client", "홀수"));
        t.add(rejected("odd for server", setupWith("\"next_stream_id\":\"3\""), "next_stream_id", "server", "짝수"));
        return t.stream();
    }

    @Test
    void acceptsNextStreamIdAtTheBoundaries() {
        assertThat(VectorLoader.parse("x.json", clientSetupWith("\"next_stream_id\":\"1\"")))
                .hasSize(1);
        assertThat(VectorLoader.parse("x.json", clientSetupWith("\"next_stream_id\":\"9223372036854775807\"")))
                .hasSize(1);
        assertThat(VectorLoader.parse("x.json", setupWith("\"next_stream_id\":\"2\"")))
                .hasSize(1);
        assertThat(VectorLoader.parse("x.json", setupWith("\"next_stream_id\":\"9223372036854775806\"")))
                .hasSize(1);
    }

    @TestFactory
    Stream<DynamicTest> rejectsKnownTypesViolations() {
        List<DynamicTest> t = new ArrayList<>();
        t.add(rejected("below 256", setupWith("\"known_types\":[256,255]"), "known_types"));
        t.add(rejected("above u16", setupWith("\"known_types\":[65536]"), "known_types"));
        t.add(rejected("non-integer element", setupWith("\"known_types\":[256.5]"), "known_types"));
        t.add(rejected("string element", setupWith("\"known_types\":[\"256\"]"), "known_types"));
        t.add(rejected("not an array", setupWith("\"known_types\":256"), "known_types", "배열"));
        t.add(rejected("duplicate", setupWith("\"known_types\":[256,257,256]"), "known_types", "중복", "256"));
        return t.stream();
    }

    @Test
    void acceptsKnownTypes() {
        assertThat(VectorLoader.parse("x.json", setupWith("\"known_types\":[]")))
                .hasSize(1);
        assertThat(VectorLoader.parse("x.json", setupWith("\"known_types\":[256,65535]")))
                .hasSize(1);
    }

    /** 마크다운 본문에서 ```json 코드 블록의 내용을 줄 끝 표기(LF, CRLF)와 관계없이 모은다. */
    private static List<String> jsonBlocks(String markdown) {
        Matcher m =
                Pattern.compile("```json\\r?\\n(.*?)\\r?\\n```", Pattern.DOTALL).matcher(markdown);
        List<String> blocks = new ArrayList<>();
        while (m.find()) {
            blocks.add(m.group(1));
        }
        return blocks;
    }

    @Test
    void jsonBlocksAreFoundWithLfAndCrlfLineEndings() {
        String lf = "text\n```json\n{\"a\":1}\n```\nmore\n```json\n[2]\n```\n";

        assertThat(jsonBlocks(lf)).containsExactly("{\"a\":1}", "[2]");
        assertThat(jsonBlocks(lf.replace("\n", "\r\n"))).containsExactly("{\"a\":1}", "[2]");
    }

    /** 형식 문서의 JSON 예시가 모두 로더를 통과한다. */
    @Test
    void jsonExamplesInTheFormatDocumentPassTheLoader() throws IOException {
        // 벡터 디렉터리에서 위로 올라가며 문서를 찾는다. 실행 위치와 벡터 디렉터리의 깊이에 기대지 않는다.
        Path doc = null;
        for (Path dir = VectorSuite.defaultDirectory().toAbsolutePath();
                dir != null && doc == null;
                dir = dir.getParent()) {
            Path candidate = dir.resolve("docs/specification/STRAND_CONFORMANCE_VECTORS.md");
            doc = Files.exists(candidate) ? candidate : null;
        }
        assertThat(doc).as("형식 문서를 찾을 수 없다").isNotNull();
        List<String> blocks = jsonBlocks(new String(Files.readAllBytes(doc), StandardCharsets.UTF_8));
        for (int i = 0; i < blocks.size(); i++) {
            assertThat(VectorLoader.parse("doc-example-" + (i + 1) + ".json", blocks.get(i)))
                    .isNotEmpty();
        }
        assertThat(blocks).hasSizeGreaterThanOrEqualTo(4);
    }

    @Test
    void vectorKeepsItsOwnCopiesOfSetupAndSteps() {
        ConformanceVector vector =
                VectorLoader.parse("x.json", file(VALID_VECTOR)).get(0);

        vector.setup().addProperty("role", "client");

        assertThat(vector.setup().get("role").getAsString()).isEqualTo("server");
        assertThatThrownBy(() -> vector.spec().add("x")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> vector.steps().add(null)).isInstanceOf(UnsupportedOperationException.class);
    }
}
