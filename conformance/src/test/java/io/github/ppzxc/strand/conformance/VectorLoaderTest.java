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
                + "{\"kind\":\"advance\",\"ms\":1,\"send\":[],\"events\":[]}]";

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
}
