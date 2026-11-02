package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
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
        return "{\"format\":1,\"vectors\":[" + String.join(",", vectors) + "]}";
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
    void loadsAppendixGExample() {
        String json = "{\"format\":1,\"vectors\":[{\"id\":\"ping-is-acknowledged\","
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
                + "{\"type\":\"CLOSE\",\"last_stream_id\":0,\"error_code\":\"NO_ERROR\"},"
                + "{\"type\":\"RESET\",\"stream_id\":1,\"error_code\":\"CANCEL\"},"
                + "{\"type\":256,\"flags\":[],\"stream_id\":1}],\"events\":[]},"
                + "{\"kind\":\"respond\",\"stream_id\":1,\"status\":\"OK\",\"send\":[],\"events\":[]}]";

        assertThat(VectorLoader.parse("x.json", file(vector("a", "[\"3.4\"]", SETUP, steps))))
                .hasSize(1);
    }

    @Test
    void acceptsOptionalFieldsWhenPresent() {
        String setup = "{\"role\":\"client\",\"established\":false,"
                + "\"local_hello\":{\"max_frame_size\":16384,\"max_pending_bytes\":16384,"
                + "\"max_concurrent_streams\":1,\"capabilities\":0,\"content_types\":1,\"content_type\":1},"
                + "\"peer_hello\":{},\"config\":{\"draining_retention_limit\":1,\"hello_timeout_ms\":5000}}";
        String steps = "["
                + "{\"kind\":\"request\",\"type\":257,\"extensions\":[{\"tag\":1,\"value\":\"0a\"}],\"payload\":\"0b\","
                + "\"send\":[{\"type\":\"PREFACE\"},{\"type\":257,\"flags\":[\"END\",\"RESPONSE\"],\"stream_id\":1,"
                + "\"status\":17,\"extensions\":[{\"tag\":1,\"value\":\"\"}],\"payload\":\"\"},"
                + "{\"type\":\"HELLO\",\"flags\":[\"ACK\"],\"stream_id\":0,\"params\":{\"max_frame_size\":1}},"
                + "{\"type\":\"CLOSE\",\"stream_id\":0,\"last_stream_id\":0,\"error_code\":\"NO_ERROR\"},"
                + "{\"type\":\"RESET\",\"stream_id\":1,\"error_code\":9}],"
                + "\"events\":[{\"event\":\"request_received\",\"stream_id\":1,\"type\":257},"
                + "{\"event\":\"result\",\"stream_id\":1,\"status\":\"OK\"},{\"event\":\"closed\"}]},"
                + "{\"kind\":\"respond\",\"stream_id\":2147483649,\"status\":\"INTERNAL\",\"send\":[],\"events\":[]},"
                + "{\"kind\":\"cancel\",\"stream_id\":1,\"send\":[],\"events\":[]},"
                + "{\"kind\":\"close\",\"error_code\":\"NO_ERROR\",\"send\":[],\"events\":[]},"
                + "{\"kind\":\"advance\",\"ms\":1,\"send\":[],\"events\":[]},"
                + "{\"kind\":\"disconnect\",\"send\":[],\"events\":[{\"event\":\"closed\"}]}]";

        assertThat(VectorLoader.parse("x.json", file(vector("a", "[\"2.4.2\",\"5.1\"]", setup, steps))))
                .hasSize(1);
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
        t.add(rejected("higher format", "{\"format\":2,\"vectors\":[]}", "format"));
        t.add(rejected("lower format", "{\"format\":0,\"vectors\":[]}", "format"));
        t.add(rejected("non-integer format", "{\"format\":\"1\",\"vectors\":[]}", "format"));
        t.add(rejected("missing format", "{\"vectors\":[]}", "필수 필드 누락", "format"));
        t.add(rejected("missing vectors", "{\"format\":1}", "필수 필드 누락", "vectors"));
        t.add(rejected("unknown top-level field", "{\"format\":1,\"vectors\":[],\"x\":1}", "알 수 없는 필드", "x"));
        t.add(rejected("legacy top-level array", "[]", "객체"));
        t.add(rejected("empty file", "  \n", "비어"));
        t.add(rejected("syntax error", "{\"format\":", "JSON"));
        t.add(rejected("trailing content", "{\"format\":1,\"vectors\":[]} []", "JSON"));
        t.add(rejected("duplicate json key", "{\"format\":1,\"format\":1,\"vectors\":[]}", "중복 키", "format"));
        t.add(rejected("vector not an object", "{\"format\":1,\"vectors\":[\"a\"]}", "[0]", "객체"));
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
                fileWithSetup("{\"role\":\"client\",\"established\":true,\"config\":{\"hello_timeout_ms\":0}}"),
                "hello_timeout_ms",
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
                fileWithStep("\"kind\":\"respond\",\"stream_id\":1,\"status\":\"NOPE\",\"send\":[],\"events\":[]"),
                "status",
                "a"));
        t.add(rejected(
                "respond reserved status below 17",
                fileWithStep("\"kind\":\"respond\",\"stream_id\":1,\"status\":16,\"send\":[],\"events\":[]"),
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
                "stream_id out of u32",
                fileWithStep("\"kind\":\"cancel\",\"stream_id\":4294967296,\"send\":[],\"events\":[]"),
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
        t.add(rejected(
                "app frame type below 256", recvWithSend("{\"type\":3,\"flags\":[],\"stream_id\":1}"), "type", "a"));
        t.add(rejected("app frame missing flags", recvWithSend("{\"type\":256,\"stream_id\":1}"), "flags", "a"));
        t.add(rejected(
                "app frame unknown flag",
                recvWithSend("{\"type\":256,\"flags\":[\"ACK\"],\"stream_id\":1}"),
                "flags",
                "a"));
        t.add(rejected("ping missing payload", recvWithSend("{\"type\":\"PING\"}"), "payload", "a"));
        t.add(rejected("ping short payload", recvWithSend("{\"type\":\"PING\",\"payload\":\"01\"}"), "payload", "a"));
        t.add(rejected(
                "close missing last_stream_id",
                recvWithSend("{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\"}"),
                "last_stream_id",
                "a"));
        t.add(rejected(
                "reset missing stream_id",
                recvWithSend("{\"type\":\"RESET\",\"error_code\":\"NO_ERROR\"}"),
                "stream_id",
                "a"));
        t.add(rejected("frame unknown field", recvWithSend("{\"type\":\"PREFACE\",\"x\":1}"), "x", "a"));
        t.add(rejected("unknown event", recvWithEvent("{\"event\":\"opened\"}"), "event", "a"));
        t.add(rejected(
                "result event missing status", recvWithEvent("{\"event\":\"result\",\"stream_id\":1}"), "status", "a"));
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
        assertMessage("{\"format\":2,\"vectors\":[]}", "x.json: format: 지원하지 않는 format이다(아는 것은 1): 2");
        assertThatThrownBy(() -> VectorLoader.parse("x.json", "{\"format\":1,\"vectors\":[]"))
                .hasMessageStartingWith("x.json: JSON 문법 오류: ");
        assertMessage("  ", "x.json: 파일이 비어 있다");
        assertMessage("[]", "x.json: 객체여야 한다");
        assertMessage("{\"format\":1,\"vectors\":[1]}", "x.json: [0] 객체여야 한다");
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
        t.add(rejected("hello stream_id not zero", recvWithSend("{\"type\":\"HELLO\",\"stream_id\":1}"), "stream_id"));
        t.add(rejected(
                "ping duplicate flag",
                recvWithSend("{\"type\":\"PING\",\"flags\":[\"ACK\",\"ACK\"],\"payload\":\"0102030405060708\"}"),
                "플래그 중복"));
        t.add(rejected(
                "ping stream_id not zero",
                recvWithSend("{\"type\":\"PING\",\"stream_id\":2,\"payload\":\"0102030405060708\"}"),
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
                recvWithSend("{\"type\":\"CLOSE\",\"stream_id\":1,\"last_stream_id\":0,\"error_code\":\"NO_ERROR\"}"),
                "stream_id"));
        t.add(rejected(
                "reset stream_id zero",
                recvWithSend("{\"type\":\"RESET\",\"stream_id\":0,\"error_code\":\"CANCEL\"}"),
                "stream_id"));
        t.add(rejected(
                "app frame duplicate flag",
                recvWithSend("{\"type\":256,\"flags\":[\"END\",\"END\"],\"stream_id\":1}"),
                "플래그 중복"));
        t.add(rejected(
                "app frame flags not strings", recvWithSend("{\"type\":256,\"flags\":[1],\"stream_id\":1}"), "flags"));
        t.add(rejected(
                "app frame missing stream_id", recvWithSend("{\"type\":256,\"flags\":[]}"), "필수 필드 누락", "stream_id"));
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

    /** 이름 대신 쓰는 정수의 예약 값 범위: status는 17~255, error_code는 9~2^32-1. */
    @Test
    void namedCodesAcceptOnlyTheReservedIntegerRange() {
        assertThat(parseStatus(17)).hasSize(1);
        assertThat(parseStatus(255)).hasSize(1);
        assertThatThrownBy(() -> parseStatus(16)).isInstanceOf(InvalidVectorException.class);
        assertThatThrownBy(() -> parseStatus(256)).isInstanceOf(InvalidVectorException.class);
        assertThat(parseErrorCode("9")).hasSize(1);
        assertThat(parseErrorCode("4294967295")).hasSize(1);
        assertThatThrownBy(() -> parseErrorCode("8")).isInstanceOf(InvalidVectorException.class);
        assertThatThrownBy(() -> parseErrorCode("4294967296")).isInstanceOf(InvalidVectorException.class);
    }

    private static List<ConformanceVector> parseStatus(int status) {
        return VectorLoader.parse(
                "x.json",
                fileWithStep(
                        "\"kind\":\"respond\",\"stream_id\":1,\"status\":" + status + ",\"send\":[],\"events\":[]"));
    }

    private static List<ConformanceVector> parseErrorCode(String code) {
        return VectorLoader.parse(
                "x.json", fileWithStep("\"kind\":\"close\",\"error_code\":" + code + ",\"send\":[],\"events\":[]"));
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
