package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 벡터 형식 문서가 정한 객체 형태의 단일 표. 단계 kind·프레임 type·이벤트 이름마다 필드 규칙을 한 곳에 모아 두므로, 형식에
 * 새 이름이나 필드가 생기면 이 표만 고친다.
 */
final class VectorSchema {

    /** 이 스키마가 아는 형식 버전(벡터 형식 문서 2절). */
    static final int KNOWN_FORMAT = 2;

    static final List<String> ROLES = Arrays.asList("client", "server");
    static final String DEFAULT_TRANSPORT = "tcp";
    static final List<String> TRANSPORTS = Arrays.asList("tcp", "websocket");
    private static final List<String> LOCAL_RESULT_STATUSES =
            Arrays.asList("UNAVAILABLE", "CANCELLED", "DEADLINE_EXCEEDED");
    static final List<String> CERTAINTIES = Arrays.asList("response", "not_processed", "unknown");
    static final List<String> OPCODES = Arrays.asList("binary", "text");
    static final List<String> ACK_FLAGS = Arrays.asList("ACK");

    // ---- 필드 값 규칙 ----

    private static final long U8_MAX = 255L;
    private static final long U16_MAX = 65535L;
    private static final long U32_MAX = 4294967295L;
    private static final BigInteger U64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private static final BigInteger STREAM_ID_MAX = BigInteger.ONE.shiftLeft(63).subtract(BigInteger.ONE);

    private static final FieldCheck U16 = FieldCheck.range(0, U16_MAX);
    static final FieldCheck U32 = FieldCheck.range(0, U32_MAX);
    /** 1 이상의 u32. 0이 아닌 스트림 ID와 시간(ms)에 쓴다. */
    static final FieldCheck POSITIVE_U32 = FieldCheck.range(1, U32_MAX);
    /** 애플리케이션 프레임·요청의 type. 0~255는 제어 프레임용으로 예약돼 있다. */
    static final FieldCheck APP_TYPE = FieldCheck.range(Protocol.FIRST_APPLICATION_TYPE, U16_MAX);
    /** 2.5.1절의 이름이거나, 이름 목록 다음부터 255까지의 예약 정수. */
    static final FieldCheck STATUS = FieldCheck.named(Protocol.STATUS_NAMES, U8_MAX);
    /** 2.5.2절의 이름이거나, 이름 목록 다음부터 u32 끝까지의 예약 정수. */
    static final FieldCheck ERROR_CODE = FieldCheck.named(Protocol.ERROR_CODE_NAMES, U32_MAX);
    /** 스트림 번호. 0 이상 2^63-1 이하의 10진수 문자열(번호는 JSON 수로 쓰지 않는다). */
    static final FieldCheck STREAM_ID = FieldCheck.decimal(BigInteger.ZERO, STREAM_ID_MAX);
    /** CLOSE의 last_stream_id. 0 이상 2^64-1 이하의 10진수 문자열(형태가 잘못된 CLOSE를 위해 번호 범위 밖 값도 쓴다). */
    static final FieldCheck LAST_STREAM_ID = FieldCheck.decimal(BigInteger.ZERO, U64_MAX);
    /** 1 이상 2^63-1 이하의 10진수 문자열. RESET의 stream_id와 next_stream_id에 쓴다(홀짝은 role과 함께 보는 검사라 {@link #setup}이 따로 본다). */
    static final FieldCheck POSITIVE_STREAM_ID = FieldCheck.decimal(BigInteger.ONE, STREAM_ID_MAX);
    /** 제어 프레임의 stream_id처럼 값이 0으로 고정된 필드. 문자열 {@code "0"}이다. */
    static final FieldCheck ZERO_STREAM_ID = FieldCheck.decimal(BigInteger.ZERO, BigInteger.ZERO);
    /** WebSocket Close의 상태 코드. 규격 1.2절이 정한 {@code 1000 Normal Closure}만 모델링한다. */
    static final FieldCheck WEBSOCKET_CLOSE_CODE = FieldCheck.range(Protocol.NORMAL_CLOSURE, Protocol.NORMAL_CLOSURE);

    private static final FieldCheck FORMAT = (c, path, element) -> c.format(path, element, KNOWN_FORMAT);

    static final Shape FILE = Shape.EMPTY.require("format", FORMAT).require("vectors", Check::array);

    static final Shape VECTOR = Shape.EMPTY
            .require("id", Check::id)
            .require("description", Check::nonEmptyText)
            .require("spec", Check::specSections)
            .require("setup", VectorSchema::setup)
            .require("steps", VectorSchema::steps);

    static final Shape EXTENSION_ENTRY = Shape.EMPTY.require("tag", U16).require("value", Check::hex);

    /** HELLO 파라미터 이름은 모두 u32다. 이름 목록은 {@link Protocol#HELLO_DEFAULTS}에서 나온다. */
    static final Shape HELLO_PARAMS = helloShape();

    private static Shape helloShape() {
        Shape shape = Shape.EMPTY;
        for (String name : Protocol.HELLO_DEFAULTS.keySet()) {
            shape = shape.allow(name, U32);
        }
        return shape;
    }

    /** config 키는 모두 1 이상의 시간(ms)이다. 키 목록은 {@link Protocol#CONFIG_DEFAULTS}에서 나온다. */
    static final Shape SETUP_CONFIG = configShape();

    private static Shape configShape() {
        Shape shape = Shape.EMPTY;
        for (String key : Protocol.CONFIG_DEFAULTS.keySet()) {
            shape = shape.allow(key, POSITIVE_U32);
        }
        return shape;
    }

    /** {@code setup}이 허용하는 키. 러너의 지원 검사가 이 목록을 쓴다. */
    static List<String> setupKeys() {
        return new ArrayList<>(SETUP.names());
    }

    static final Shape AUTH_SETUP =
            Shape.EMPTY.allow("mechanism", Check::mechanism).allow("credentials", Check::hex);

    static final Shape AUTH_PARAMS = Shape.EMPTY.allow("credential_lifetime", U32);

    private static final Shape SETUP = Shape.EMPTY
            .require("role", FieldCheck.oneOf(ROLES))
            .require("established", Check::bool)
            .allow("local_hello", FieldCheck.object(HELLO_PARAMS))
            .allow("peer_hello", FieldCheck.object(HELLO_PARAMS))
            .allow("config", FieldCheck.object(SETUP_CONFIG))
            .allow("auth", FieldCheck.object(AUTH_SETUP))
            .allow("transport", FieldCheck.oneOf(TRANSPORTS))
            .allow("next_stream_id", POSITIVE_STREAM_ID)
            .allow("known_types", Check::uniqueAppTypes);

    // ---- 단계: kind별 필드는 StepKind가 정의한다 ----

    // ---- 프레임: type별 필드 ----

    static final String FRAME_TAG = "type";
    private static final String FRAME_LABEL = "프레임 type";

    // ---- 이벤트: 이름별 필드는 EventKind가 정의한다 ----

    private static final String EVENT_LABEL = EventKind.TAG;

    private VectorSchema() {}

    /** 필드별 검사에 더해, 필드 사이의 관계(next_stream_id의 홀짝)를 본다. */
    static void setup(Check c, String path, JsonElement element) {
        JsonObject setup = c.object(path, element, SETUP);
        if (setup.has("next_stream_id")) {
            boolean client = "client".equals(setup.get("role").getAsString());
            BigInteger next = new BigInteger(setup.get("next_stream_id").getAsString());
            if (next.testBit(0) != client) {
                throw c.failAt(
                        Check.child(path, "next_stream_id"),
                        "role이 " + (client ? "client이면 홀수" : "server이면 짝수") + "여야 한다: " + next);
            }
        }
    }

    /** 벡터 안에서 setup과 단계가 함께 정하는 규칙: transport가 tcp면 recv만, websocket이면 recv_message만 쓴다. */
    static void crossCheck(Check c, JsonObject vector) {
        JsonObject setup = vector.getAsJsonObject("setup");
        String transport = setup.has("transport") ? setup.get("transport").getAsString() : DEFAULT_TRANSPORT;
        JsonArray steps = vector.getAsJsonArray("steps");
        boolean server = "server".equals(setup.get("role").getAsString());
        boolean[] authAckReceived = AuthGate.ackReceivedThrough(vector);
        boolean authGateOpen = false;
        for (int i = 0; i < steps.size(); i++) {
            JsonObject step = steps.get(i).getAsJsonObject();
            StepKind kind = StepKind.of(step);
            if (kind.onlyOnTransport() != null && !kind.onlyOnTransport().equals(transport)) {
                throw c.failAt(
                        Check.child(Check.item("steps", i), StepKind.TAG),
                        "transport가 " + transport + "인 벡터에는 " + kind.name() + " 단계를 쓸 수 없다");
            }
            // 벡터 형식 문서 8.1절 3번: 확립 전 벡터는 AUTH 관문이 열리기 전에 ping 단계를 쓰지 않는다.
            if (kind.sendsPing() && !setup.get("established").getAsBoolean() && !authGateOpen) {
                throw c.failAt(
                        Check.child(Check.item("steps", i), StepKind.TAG),
                        "established가 false인 벡터는 AUTH 관문 통과(서버 역할은 send의 AUTH ACK, 클라이언트 역할은 recv한 AUTH ACK) 이전에 ping 단계를 쓸 수 없다");
            }
            if (transport.equals("tcp")) {
                rejectWebSocketClose(c, i, step);
            }
            authGateOpen |= server ? sendsAuthAck(step) : authAckReceived[i];
        }
    }

    /** WebSocket Close는 WebSocket 전송의 출력 항목이다(벡터 형식 문서 6.1절). */
    private static void rejectWebSocketClose(Check c, int stepIndex, JsonObject step) {
        JsonArray send = step.getAsJsonArray("send");
        for (int j = 0; j < send.size(); j++) {
            if ("WEBSOCKET_CLOSE".equals(frameType(send.get(j).getAsJsonObject()))) {
                throw c.failAt(
                        Check.child(Check.item(Check.child(Check.item("steps", stepIndex), "send"), j), FRAME_TAG),
                        "transport가 tcp인 벡터에는 WEBSOCKET_CLOSE 항목을 쓸 수 없다");
            }
        }
    }

    private static boolean sendsAuthAck(JsonObject step) {
        for (JsonElement frame : step.getAsJsonArray("send")) {
            JsonObject f = frame.getAsJsonObject();
            if ("AUTH".equals(frameType(f)) && hasFlag(f, "ACK")) {
                return true;
            }
        }
        return false;
    }

    /** 프레임 항목의 `type`이 문자열 이름이면 그 이름, 아니면 null(숫자 type이거나 깨진 값). */
    static String frameType(JsonObject frame) {
        JsonElement type = frame.get(FRAME_TAG);
        return type != null
                        && type.isJsonPrimitive()
                        && type.getAsJsonPrimitive().isString()
                ? type.getAsString()
                : null;
    }

    /** 프레임 항목의 `flags`에 이 이름이 있는가. `flags`가 없거나 배열이 아니면 false. */
    static boolean hasFlag(JsonObject frame, String flag) {
        JsonElement flags = frame.get("flags");
        return flags != null && flags.isJsonArray() && flags.getAsJsonArray().contains(new JsonPrimitive(flag));
    }

    /** 벡터 형식 문서 6.2절: `stream_id` 없는 result는 UNAVAILABLE, CANCELLED, DEADLINE_EXCEEDED 중 하나이고 not_processed다. */
    static void localResult(Check c, String path, JsonObject event) {
        if (event.has("stream_id")) {
            return;
        }
        if (!LOCAL_RESULT_STATUSES.contains(event.get("status").getAsString())) {
            throw c.failAt(
                    Check.child(path, "status"),
                    "stream_id 없는 result의 status는 UNAVAILABLE, CANCELLED, DEADLINE_EXCEEDED 중 하나여야 한다");
        }
        if (!"not_processed".equals(event.get("certainty").getAsString())) {
            throw c.failAt(Check.child(path, "certainty"), "stream_id 없는 result의 certainty는 not_processed여야 한다");
        }
    }

    static void steps(Check c, String path, JsonElement element) {
        JsonArray array = c.array(path, element);
        if (array.isEmpty()) {
            throw c.failAt(path, "하나 이상이어야 한다");
        }
        for (int i = 0; i < array.size(); i++) {
            String stepPath = Check.item(path, i);
            c.objectNamedBy(stepPath, array.get(i), StepKind.TAG, StepKind.LABEL, StepKind.shapesByName());
        }
    }

    static void frames(Check c, String path, JsonElement element) {
        JsonArray array = c.array(path, element);
        for (int i = 0; i < array.size(); i++) {
            frame(c, Check.item(path, i), array.get(i));
        }
    }

    static void events(Check c, String path, JsonElement element) {
        JsonArray array = c.array(path, element);
        for (int i = 0; i < array.size(); i++) {
            String eventPath = Check.item(path, i);
            JsonObject event =
                    c.objectNamedBy(eventPath, array.get(i), EventKind.TAG, EVENT_LABEL, EventKind.shapesByName());
            if ("result".equals(event.get(EventKind.TAG).getAsString())) {
                localResult(c, eventPath, event);
            }
        }
    }

    /** type이 이름이면 그 이름의 제어 프레임, 정수이면 애플리케이션 프레임이다. */
    private static void frame(Check c, String path, JsonElement element) {
        JsonElement type = c.tag(path, element, FRAME_TAG);
        String typePath = Check.child(path, FRAME_TAG);
        Shape shape;
        if (Check.isString(type)) {
            shape = c.shapeFor(typePath, FRAME_LABEL, FrameKind.shapesByName(), type.getAsString());
        } else {
            APP_TYPE.check(c, typePath, type);
            shape = FrameKind.applicationShape();
        }
        JsonObject frame = c.object(path, element, shape);
        if ("AUTH".equals(frameType(frame))) {
            authFrame(c, path, frame);
        }
    }

    /** 벡터 형식 문서 6.1절: AUTH는 mechanism·credentials를, AUTH(ACK)는 params를 쓴다. 두 쪽의 필드를 섞을 수 없다. */
    private static void authFrame(Check c, String path, JsonObject frame) {
        if (hasFlag(frame, "ACK")) {
            for (String field : new String[] {"mechanism", "credentials"}) {
                if (frame.has(field)) {
                    throw c.failAt(Check.child(path, field), "AUTH(ACK) 항목에는 " + field + "를 쓸 수 없다(ACK이면 params만 쓴다)");
                }
            }
        } else {
            if (!frame.has("mechanism")) {
                throw c.failAt(path, "필수 필드 누락: mechanism (ACK가 아닌 AUTH 항목)");
            }
            if (frame.has("params")) {
                throw c.failAt(Check.child(path, "params"), "ACK가 아닌 AUTH 항목에는 params를 쓸 수 없다(params는 ACK일 때만 쓴다)");
            }
        }
    }
}
