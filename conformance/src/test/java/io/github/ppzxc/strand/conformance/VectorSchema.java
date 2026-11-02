package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/** 벡터 형식 문서가 정한 객체 형태와 벡터 안쪽(setup·steps)의 검증 규칙. */
final class VectorSchema {

    private static final Pattern PING_PAYLOAD = Pattern.compile("[0-9a-f]{16}");
    private static final List<String> APP_FLAGS = Arrays.asList("RESPONSE", "END");
    private static final List<String> ACK_FLAGS = Arrays.asList("ACK");

    static final Shape FILE = Shape.EMPTY.require("format", FieldKind.DEFERRED).require("vectors", FieldKind.ARRAY);

    static final Shape VECTOR = Shape.EMPTY
            .require("id", FieldKind.DEFERRED)
            .require("description", FieldKind.DEFERRED)
            .require("spec", FieldKind.DEFERRED)
            .require("setup", FieldKind.DEFERRED)
            .require("steps", FieldKind.DEFERRED);

    static final Shape EXTENSION_ENTRY =
            Shape.EMPTY.require("tag", FieldKind.U16).require("value", FieldKind.HEX);

    static final Shape HELLO_PARAMS = Shape.EMPTY
            .allow("max_frame_size", FieldKind.U32)
            .allow("max_pending_bytes", FieldKind.U32)
            .allow("max_concurrent_streams", FieldKind.U32)
            .allow("capabilities", FieldKind.U32)
            .allow("content_types", FieldKind.U32)
            .allow("content_type", FieldKind.U32);

    private static final Shape SETUP = Shape.EMPTY
            .require("role", FieldKind.DEFERRED)
            .require("established", FieldKind.DEFERRED)
            .allow("local_hello", FieldKind.HELLO_PARAMS)
            .allow("peer_hello", FieldKind.HELLO_PARAMS)
            .allow("config", FieldKind.DEFERRED);

    private static final Shape SETUP_CONFIG = Shape.EMPTY
            .allow("draining_retention_limit", FieldKind.POSITIVE_U32)
            .allow("hello_timeout_ms", FieldKind.POSITIVE_U32);

    private VectorSchema() {}

    static JsonObject setup(Check c, JsonElement element) {
        JsonObject setup = c.object("setup", element, SETUP);
        String role = c.string("setup.role", setup.get("role"));
        if (!role.equals("client") && !role.equals("server")) {
            throw c.failAt("setup.role", "client 또는 server여야 한다: " + role);
        }
        c.bool("setup.established", setup.get("established"));
        if (setup.has("config")) {
            c.object("setup.config", setup.get("config"), SETUP_CONFIG);
        }
        return setup;
    }

    static JsonObject step(Check c, String path, JsonElement element) {
        if (!element.isJsonObject()) {
            throw c.failAt(path, "객체여야 한다");
        }
        JsonElement kindElement = element.getAsJsonObject().get("kind");
        if (kindElement == null) {
            throw c.failAt(path, "필수 필드 누락: kind");
        }
        String kind = c.string(Check.child(path, "kind"), kindElement);
        Shape shape = Shape.EMPTY
                .require("kind", FieldKind.DEFERRED)
                .require("send", FieldKind.DEFERRED)
                .require("events", FieldKind.DEFERRED);
        switch (kind) {
            case "recv":
                shape = shape.require("hex", FieldKind.HEX);
                break;
            case "request":
                shape = shape.require("type", FieldKind.APP_TYPE)
                        .allow("extensions", FieldKind.EXTENSIONS)
                        .allow("payload", FieldKind.HEX);
                break;
            case "respond":
                shape = shape.require("stream_id", FieldKind.U32)
                        .require("status", FieldKind.STATUS)
                        .allow("extensions", FieldKind.EXTENSIONS)
                        .allow("payload", FieldKind.HEX);
                break;
            case "cancel":
                shape = shape.require("stream_id", FieldKind.U32);
                break;
            case "close":
                shape = shape.require("error_code", FieldKind.ERROR_CODE);
                break;
            case "advance":
                shape = shape.require("ms", FieldKind.POSITIVE_U32);
                break;
            default:
                throw c.failAt(Check.child(path, "kind"), "알 수 없는 단계 kind: " + kind);
        }
        JsonObject step = c.object(path, element, shape);
        JsonArray send = c.array(Check.child(path, "send"), step.get("send"));
        for (int i = 0; i < send.size(); i++) {
            frame(c, Check.item(Check.child(path, "send"), i), send.get(i));
        }
        JsonArray events = c.array(Check.child(path, "events"), step.get("events"));
        for (int i = 0; i < events.size(); i++) {
            event(c, Check.item(Check.child(path, "events"), i), events.get(i));
        }
        return step;
    }

    private static void frame(Check c, String path, JsonElement element) {
        if (!element.isJsonObject()) {
            throw c.failAt(path, "객체여야 한다");
        }
        JsonElement typeElement = element.getAsJsonObject().get("type");
        if (typeElement == null) {
            throw c.failAt(path, "필수 필드 누락: type");
        }
        Shape shape = Shape.EMPTY.require("type", FieldKind.DEFERRED);
        boolean appFrame = false;
        if (Check.isString(typeElement)) {
            switch (typeElement.getAsString()) {
                case "PREFACE":
                    break;
                case "HELLO":
                    shape = shape.allow("flags", FieldKind.DEFERRED)
                            .allow("stream_id", FieldKind.DEFERRED)
                            .allow("params", FieldKind.HELLO_PARAMS);
                    break;
                case "PING":
                    shape = shape.require("payload", FieldKind.DEFERRED)
                            .allow("flags", FieldKind.DEFERRED)
                            .allow("stream_id", FieldKind.DEFERRED);
                    break;
                case "CLOSE":
                    shape = shape.require("last_stream_id", FieldKind.U32)
                            .require("error_code", FieldKind.ERROR_CODE)
                            .allow("stream_id", FieldKind.DEFERRED);
                    break;
                case "RESET":
                    shape = shape.require("stream_id", FieldKind.U32).require("error_code", FieldKind.ERROR_CODE);
                    break;
                default:
                    throw c.failAt(Check.child(path, "type"), "알 수 없는 프레임 type: " + typeElement.getAsString());
            }
        } else {
            FieldKind.APP_TYPE.check(c, Check.child(path, "type"), typeElement);
            appFrame = true;
            shape = shape.require("flags", FieldKind.DEFERRED)
                    .require("stream_id", FieldKind.U32)
                    .allow("status", FieldKind.STATUS)
                    .allow("extensions", FieldKind.EXTENSIONS)
                    .allow("payload", FieldKind.HEX);
        }
        JsonObject frame = c.object(path, element, shape);
        String name = Check.isString(typeElement) ? typeElement.getAsString() : "";
        String flagsPath = Check.child(path, "flags");
        String streamIdPath = Check.child(path, "stream_id");
        if (appFrame) {
            c.flags(flagsPath, frame.get("flags"), APP_FLAGS);
        } else if (name.equals("HELLO") || name.equals("PING")) {
            if (frame.has("flags")) {
                c.flags(flagsPath, frame.get("flags"), ACK_FLAGS);
            }
            if (frame.has("stream_id")) {
                c.range(streamIdPath, frame.get("stream_id"), 0, 0);
            }
        }
        if (name.equals("PING")) {
            String payloadPath = Check.child(path, "payload");
            String payload = c.string(payloadPath, frame.get("payload"));
            if (!PING_PAYLOAD.matcher(payload).matches()) {
                throw c.failAt(payloadPath, "공백 없는 소문자 16자리 hex여야 한다: '" + payload + "'");
            }
        }
        if (name.equals("CLOSE") && frame.has("stream_id")) {
            c.range(streamIdPath, frame.get("stream_id"), 0, 0);
        }
        if (name.equals("RESET")) {
            c.range(streamIdPath, frame.get("stream_id"), 1, FieldKind.U32_MAX);
        }
    }

    private static void event(Check c, String path, JsonElement element) {
        if (!element.isJsonObject()) {
            throw c.failAt(path, "객체여야 한다");
        }
        JsonElement nameElement = element.getAsJsonObject().get("event");
        if (nameElement == null) {
            throw c.failAt(path, "필수 필드 누락: event");
        }
        String name = c.string(Check.child(path, "event"), nameElement);
        Shape shape = Shape.EMPTY.require("event", FieldKind.DEFERRED);
        switch (name) {
            case "request_received":
                shape = shape.require("stream_id", FieldKind.U32).require("type", FieldKind.APP_TYPE);
                break;
            case "result":
                shape = shape.require("stream_id", FieldKind.U32).require("status", FieldKind.STATUS);
                break;
            case "closed":
                break;
            default:
                throw c.failAt(Check.child(path, "event"), "알 수 없는 event: " + name);
        }
        c.object(path, element, shape);
    }
}
