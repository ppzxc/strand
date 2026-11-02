package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
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
    static final List<String> APP_FLAGS = Arrays.asList("RESPONSE", "END");
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
            .allow("max_pending_bytes", FieldKind.U32)
            .allow("max_concurrent_streams", FieldKind.U32)
            .allow("capabilities", FieldKind.U32)
            .allow("content_types", FieldKind.U32)
            .allow("content_type", FieldKind.U32);

    static final Shape SETUP_CONFIG = Shape.EMPTY
            .allow("draining_retention_limit", FieldKind.POSITIVE_U32)
            .allow("hello_timeout_ms", FieldKind.POSITIVE_U32);

    private static final Shape SETUP = Shape.EMPTY
            .require("role", FieldKind.ROLE)
            .require("established", FieldKind.BOOLEAN)
            .allow("local_hello", FieldKind.HELLO_PARAMS)
            .allow("peer_hello", FieldKind.HELLO_PARAMS)
            .allow("config", FieldKind.SETUP_CONFIG);

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
            .require("flags", FieldKind.APP_FLAGS)
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
                "CLOSE",
                FRAME_BASE
                        .require("last_stream_id", FieldKind.U32)
                        .require("error_code", FieldKind.ERROR_CODE)
                        .allow("stream_id", FieldKind.ZERO));
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

    static void setup(Check c, String path, JsonElement element) {
        c.object(path, element, SETUP);
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
