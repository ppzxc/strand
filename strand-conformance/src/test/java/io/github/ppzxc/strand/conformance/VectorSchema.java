package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
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
    static final List<String> CERTAINTIES = Arrays.asList("response", "not_processed", "unknown");
    static final List<String> OPCODES = Arrays.asList("binary", "text");
    static final List<String> ACK_FLAGS = Arrays.asList("ACK");

    static final Shape FILE = Shape.EMPTY.require("format", FieldKind.FORMAT).require("vectors", FieldKind.ARRAY);

    static final Shape VECTOR = Shape.EMPTY
            .require("id", FieldKind.VECTOR_ID)
            .require("description", FieldKind.DESCRIPTION)
            .require("spec", FieldKind.SPEC_SECTIONS)
            .require("setup", FieldKind.SETUP)
            .require("steps", FieldKind.STEPS);

    static final Shape EXTENSION_ENTRY =
            Shape.EMPTY.require("tag", FieldKind.U16).require("value", FieldKind.HEX);

    static final Shape HELLO_PARAMS = Shape.EMPTY
            .allow("max_frame_size", FieldKind.U32)
            .allow("max_concurrent_streams", FieldKind.U32)
            .allow("capabilities", FieldKind.U32);

    static final Shape SETUP_CONFIG = Shape.EMPTY
            .allow("ack_timeout_ms", FieldKind.POSITIVE_U32)
            .allow("auth_timeout_ms", FieldKind.POSITIVE_U32);

    static final Shape AUTH_SETUP =
            Shape.EMPTY.allow("mechanism", FieldKind.MECHANISM).allow("credentials", FieldKind.HEX);

    static final Shape AUTH_PARAMS = Shape.EMPTY.allow("credential_lifetime", FieldKind.U32);

    private static final Shape SETUP = Shape.EMPTY
            .require("role", FieldKind.ROLE)
            .require("established", FieldKind.BOOLEAN)
            .allow("local_hello", FieldKind.HELLO_PARAMS)
            .allow("peer_hello", FieldKind.HELLO_PARAMS)
            .allow("config", FieldKind.SETUP_CONFIG)
            .allow("auth", FieldKind.AUTH_SETUP)
            .allow("transport", FieldKind.TRANSPORT)
            .allow("next_stream_id", FieldKind.POSITIVE_STREAM_ID)
            .allow("known_types", FieldKind.KNOWN_TYPES);

    // ---- 단계: kind별 필드 ----

    private static final String STEP_TAG = "kind";
    private static final String STEP_LABEL = "단계 kind";

    private static final Shape STEP_BASE = Shape.EMPTY
            .require(STEP_TAG, FieldKind.DEFERRED)
            .require("send", FieldKind.FRAMES)
            .require("events", FieldKind.EVENTS);

    private static final Map<String, Shape> STEPS_BY_KIND = new HashMap<>();

    static {
        STEPS_BY_KIND.put("recv", STEP_BASE.require("hex", FieldKind.HEX));
        STEPS_BY_KIND.put(
                "recv_message", STEP_BASE.require("opcode", FieldKind.OPCODE).require("hex", FieldKind.HEX));
        STEPS_BY_KIND.put("ping", STEP_BASE.require("payload", FieldKind.PING_PAYLOAD));
        STEPS_BY_KIND.put(
                "request",
                STEP_BASE
                        .require("type", FieldKind.APP_TYPE)
                        .allow("extensions", FieldKind.EXTENSIONS)
                        .allow("payload", FieldKind.HEX));
        STEPS_BY_KIND.put(
                "respond",
                STEP_BASE
                        .require("stream_id", FieldKind.STREAM_ID)
                        .require("status", FieldKind.STATUS)
                        .allow("extensions", FieldKind.EXTENSIONS)
                        .allow("payload", FieldKind.HEX));
        STEPS_BY_KIND.put("cancel", STEP_BASE.require("stream_id", FieldKind.STREAM_ID));
        STEPS_BY_KIND.put("close", STEP_BASE.require("error_code", FieldKind.ERROR_CODE));
        STEPS_BY_KIND.put("advance", STEP_BASE.require("ms", FieldKind.POSITIVE_U32));
        STEPS_BY_KIND.put("disconnect", STEP_BASE);
    }

    // ---- 프레임: type별 필드 ----

    private static final String FRAME_TAG = "type";
    private static final String FRAME_LABEL = "프레임 type";

    private static final Shape FRAME_BASE = Shape.EMPTY.require(FRAME_TAG, FieldKind.DEFERRED);

    private static final Map<String, Shape> FRAMES_BY_TYPE = new HashMap<>();

    /** type이 256 이상의 정수인 애플리케이션 프레임. */
    private static final Shape APP_FRAME = FRAME_BASE
            .require("stream_id", FieldKind.STREAM_ID)
            .allow("status", FieldKind.STATUS)
            .allow("extensions", FieldKind.EXTENSIONS)
            .allow("payload", FieldKind.HEX);

    static {
        FRAMES_BY_TYPE.put("PREFACE", FRAME_BASE);
        FRAMES_BY_TYPE.put(
                "HELLO",
                FRAME_BASE
                        .allow("flags", FieldKind.ACK_FLAGS)
                        .allow("stream_id", FieldKind.ZERO_STREAM_ID)
                        .allow("params", FieldKind.HELLO_PARAMS));
        FRAMES_BY_TYPE.put(
                "AUTH",
                FRAME_BASE
                        .allow("flags", FieldKind.ACK_FLAGS)
                        .allow("stream_id", FieldKind.ZERO_STREAM_ID)
                        .allow("mechanism", FieldKind.MECHANISM)
                        .allow("credentials", FieldKind.HEX)
                        .allow("params", FieldKind.AUTH_PARAMS));
        FRAMES_BY_TYPE.put(
                "PING",
                FRAME_BASE
                        .require("payload", FieldKind.PING_PAYLOAD)
                        .allow("flags", FieldKind.ACK_FLAGS)
                        .allow("stream_id", FieldKind.ZERO_STREAM_ID));
        FRAMES_BY_TYPE.put(
                "CLOSE",
                FRAME_BASE
                        .require("error_code", FieldKind.ERROR_CODE)
                        .require("last_stream_id", FieldKind.LAST_STREAM_ID)
                        .allow("stream_id", FieldKind.ZERO_STREAM_ID));
        FRAMES_BY_TYPE.put("WEBSOCKET_CLOSE", FRAME_BASE.require("code", FieldKind.WEBSOCKET_CLOSE_CODE));
        FRAMES_BY_TYPE.put(
                "RESET",
                FRAME_BASE
                        .require("stream_id", FieldKind.POSITIVE_STREAM_ID)
                        .require("error_code", FieldKind.ERROR_CODE));
    }

    // ---- 이벤트: 이름별 필드 ----

    private static final String EVENT_TAG = "event";
    private static final String EVENT_LABEL = "event";

    private static final Shape EVENT_BASE = Shape.EMPTY.require(EVENT_TAG, FieldKind.DEFERRED);

    private static final Map<String, Shape> EVENTS_BY_NAME = new HashMap<>();

    static {
        EVENTS_BY_NAME.put(
                "request_received",
                EVENT_BASE.require("stream_id", FieldKind.STREAM_ID).require("type", FieldKind.APP_TYPE));
        EVENTS_BY_NAME.put(
                "result",
                EVENT_BASE
                        .allow("stream_id", FieldKind.STREAM_ID)
                        .require("status", FieldKind.STATUS)
                        .require("certainty", FieldKind.CERTAINTY));
        EVENTS_BY_NAME.put("closed", EVENT_BASE);
    }

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
        String forbidden = transport.equals("tcp") ? "recv_message" : "recv";
        JsonArray steps = vector.getAsJsonArray("steps");
        boolean server = "server".equals(setup.get("role").getAsString());
        boolean[] authAckReceived = AckWaits.authAckReceivedThrough(vector);
        boolean authGateOpen = false;
        for (int i = 0; i < steps.size(); i++) {
            JsonObject step = steps.get(i).getAsJsonObject();
            String kind = step.get(STEP_TAG).getAsString();
            if (kind.equals(forbidden)) {
                throw c.failAt(
                        Check.child(Check.item("steps", i), STEP_TAG),
                        "transport가 " + transport + "인 벡터에는 " + forbidden + " 단계를 쓸 수 없다");
            }
            // 벡터 형식 문서 8.1절 3번: 확립 전 벡터는 AUTH 관문이 열리기 전에 ping 단계를 쓰지 않는다.
            if (kind.equals("ping") && !setup.get("established").getAsBoolean() && !authGateOpen) {
                throw c.failAt(
                        Check.child(Check.item("steps", i), STEP_TAG),
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

    /** 벡터 형식 문서 6.2절: `stream_id` 없는 result는 UNAVAILABLE이고 not_processed다. */
    static void localResult(Check c, String path, JsonObject event) {
        if (event.has("stream_id")) {
            return;
        }
        if (!"UNAVAILABLE".equals(event.get("status").getAsString())) {
            throw c.failAt(Check.child(path, "status"), "stream_id 없는 result의 status는 UNAVAILABLE이어야 한다");
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
            c.objectNamedBy(stepPath, array.get(i), STEP_TAG, STEP_LABEL, STEPS_BY_KIND);
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
            JsonObject event = c.objectNamedBy(eventPath, array.get(i), EVENT_TAG, EVENT_LABEL, EVENTS_BY_NAME);
            if ("result".equals(event.get(EVENT_TAG).getAsString())) {
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
            shape = c.shapeFor(typePath, FRAME_LABEL, FRAMES_BY_TYPE, type.getAsString());
        } else {
            FieldKind.APP_TYPE.check(c, typePath, type);
            shape = APP_FRAME;
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
