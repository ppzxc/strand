package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
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
    static final int KNOWN_FORMAT = 1;

    static final List<String> ROLES = Arrays.asList("client", "server");
    static final String DEFAULT_TRANSPORT = "tcp";
    static final List<String> TRANSPORTS = Arrays.asList("tcp", "websocket");
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

    static final Shape SETUP_CONFIG = Shape.EMPTY.allow("ack_timeout_ms", FieldKind.POSITIVE_U32);

    private static final Shape SETUP = Shape.EMPTY
            .require("role", FieldKind.ROLE)
            .require("established", FieldKind.BOOLEAN)
            .allow("local_hello", FieldKind.HELLO_PARAMS)
            .allow("peer_hello", FieldKind.HELLO_PARAMS)
            .allow("config", FieldKind.SETUP_CONFIG)
            .allow("transport", FieldKind.TRANSPORT)
            .allow("next_stream_id", FieldKind.STREAM_NUMBER)
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
                        .require("stream_id", FieldKind.U32)
                        .require("status", FieldKind.STATUS)
                        .allow("extensions", FieldKind.EXTENSIONS)
                        .allow("payload", FieldKind.HEX));
        STEPS_BY_KIND.put("cancel", STEP_BASE.require("stream_id", FieldKind.U32));
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
            .require("stream_id", FieldKind.U32)
            .allow("status", FieldKind.STATUS)
            .allow("extensions", FieldKind.EXTENSIONS)
            .allow("payload", FieldKind.HEX);

    static {
        FRAMES_BY_TYPE.put("PREFACE", FRAME_BASE);
        FRAMES_BY_TYPE.put(
                "HELLO",
                FRAME_BASE
                        .allow("flags", FieldKind.ACK_FLAGS)
                        .allow("stream_id", FieldKind.ZERO)
                        .allow("params", FieldKind.HELLO_PARAMS));
        FRAMES_BY_TYPE.put(
                "PING",
                FRAME_BASE
                        .require("payload", FieldKind.PING_PAYLOAD)
                        .allow("flags", FieldKind.ACK_FLAGS)
                        .allow("stream_id", FieldKind.ZERO));
        FRAMES_BY_TYPE.put(
                "CLOSE", FRAME_BASE.require("error_code", FieldKind.ERROR_CODE).allow("stream_id", FieldKind.ZERO));
        FRAMES_BY_TYPE.put("WEBSOCKET_CLOSE", FRAME_BASE.require("code", FieldKind.WEBSOCKET_CLOSE_CODE));
        FRAMES_BY_TYPE.put(
                "RESET",
                FRAME_BASE.require("stream_id", FieldKind.POSITIVE_U32).require("error_code", FieldKind.ERROR_CODE));
    }

    // ---- 이벤트: 이름별 필드 ----

    private static final String EVENT_TAG = "event";
    private static final String EVENT_LABEL = "event";

    private static final Shape EVENT_BASE = Shape.EMPTY.require(EVENT_TAG, FieldKind.DEFERRED);

    private static final Map<String, Shape> EVENTS_BY_NAME = new HashMap<>();

    static {
        EVENTS_BY_NAME.put(
                "request_received",
                EVENT_BASE.require("stream_id", FieldKind.U32).require("type", FieldKind.APP_TYPE));
        EVENTS_BY_NAME.put(
                "result", EVENT_BASE.require("stream_id", FieldKind.U32).require("status", FieldKind.STATUS));
        EVENTS_BY_NAME.put("closed", EVENT_BASE);
    }

    private VectorSchema() {}

    /** 필드별 검사에 더해, 필드 사이의 관계(next_stream_id의 홀짝)를 본다. */
    static void setup(Check c, String path, JsonElement element) {
        JsonObject setup = c.object(path, element, SETUP);
        if (setup.has("next_stream_id")) {
            boolean client = "client".equals(setup.get("role").getAsString());
            long next = c.longOf(setup, "next_stream_id");
            if ((next % 2 == 1) != client) {
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
        boolean helloAckSent = false;
        for (int i = 0; i < steps.size(); i++) {
            JsonObject step = steps.get(i).getAsJsonObject();
            String kind = step.get(STEP_TAG).getAsString();
            if (kind.equals(forbidden)) {
                throw c.failAt(
                        Check.child(Check.item("steps", i), STEP_TAG),
                        "transport가 " + transport + "인 벡터에는 " + forbidden + " 단계를 쓸 수 없다");
            }
            // 벡터 형식 문서 8.1절 3번: 확립 전 벡터는 상대 HELLO에 ACK를 보내기 전에 ping 단계를 쓰지 않는다.
            if (kind.equals("ping") && !setup.get("established").getAsBoolean() && !helloAckSent) {
                throw c.failAt(
                        Check.child(Check.item("steps", i), STEP_TAG),
                        "established가 false인 벡터는 상대 HELLO에 대한 ACK 송신(send의 HELLO ACK) 이전에 ping 단계를 쓸 수 없다");
            }
            if (transport.equals("tcp")) {
                rejectWebSocketClose(c, i, step);
            }
            helloAckSent |= sendsHelloAck(step);
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

    private static boolean sendsHelloAck(JsonObject step) {
        for (JsonElement frame : step.getAsJsonArray("send")) {
            JsonObject f = frame.getAsJsonObject();
            if ("HELLO".equals(frameType(f)) && hasFlag(f, "ACK")) {
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
            c.objectNamedBy(eventPath, array.get(i), EVENT_TAG, EVENT_LABEL, EVENTS_BY_NAME);
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
        c.object(path, element, shape);
    }
}
