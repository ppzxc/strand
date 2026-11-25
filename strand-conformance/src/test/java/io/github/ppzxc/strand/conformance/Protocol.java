package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Protocol {

    private Protocol() {}

    static final String PREFACE_PREFIX_HEX = "c0de53545230";

    static final String PREFACE_HEX = PREFACE_PREFIX_HEX + "0000";

    static final int HEADER_SIZE = 20;
    static final int HEADER_HEX = HEADER_SIZE * 2;

    static final int LENGTH_SIZE = 4;

    static final int LENGTH_HEX = LENGTH_SIZE * 2;
    static final int TYPE_AT = 4;
    static final int TYPE_HEX_AT = TYPE_AT * 2;
    static final int FLAGS_AT = 6;
    static final int FLAGS_HEX_AT = FLAGS_AT * 2;
    static final int STATUS_AT = 7;
    static final int STREAM_ID_AT = 8;
    static final int EXTENSIONS_LENGTH_AT = 16;
    static final int EXTENSIONS_LENGTH_HEX_AT = EXTENSIONS_LENGTH_AT * 2;
    static final int RESERVED_AT = 18;

    static final int MIN_FRAME_LENGTH = 16;

    static final int CLOSE_PAYLOAD_SIZE = 12;

    static final int RESET_PAYLOAD_SIZE = 4;

    static final int PING_PAYLOAD_SIZE = 8;

    static final int ACK_FLAG = 0x01;
    static final int TYPE_HELLO = 0x0001;
    static final int TYPE_PING = 0x0002;
    static final int TYPE_CLOSE = 0x0003;
    static final int TYPE_RESET = 0x0004;
    static final int TYPE_AUTH = 0x0005;

    static final int FIRST_APPLICATION_TYPE = 256;

    static final int NORMAL_CLOSURE = 1000;

    static final String DEFAULT_AUTH_MECHANISM = "ANONYMOUS";

    static final Map<Integer, String> HELLO_PARAM_NAMES;

    static final Map<String, Long> HELLO_DEFAULTS;

    static final int PARAM_ENTRY_SIZE = 6;

    static final int AUTH_PARAM_CREDENTIAL_LIFETIME = 1;

    static int frameType(byte[] bytes) {
        return ((bytes[TYPE_AT] & 0xFF) << Byte.SIZE) | (bytes[TYPE_AT + 1] & 0xFF);
    }

    static int frameFlags(byte[] bytes) {
        return bytes[FLAGS_AT] & 0xFF;
    }

    static String streamIdFromFrame(byte[] bytes) {
        long streamId = 0;
        for (int i = STREAM_ID_AT; i < STREAM_ID_AT + Long.BYTES; i++) {
            streamId = (streamId << Byte.SIZE) | (bytes[i] & 0xFFL);
        }
        return Long.toUnsignedString(streamId);
    }

    static {
        Map<Integer, String> names = new LinkedHashMap<>();
        Map<String, Long> defaults = new LinkedHashMap<>();
        helloParam(names, defaults, 1, "max_frame_size", 65_536L);
        helloParam(names, defaults, 2, "max_concurrent_streams", 100L);
        helloParam(names, defaults, 3, "capabilities", 0L);
        HELLO_PARAM_NAMES = Collections.unmodifiableMap(names);
        HELLO_DEFAULTS = Collections.unmodifiableMap(defaults);
    }

    private static void helloParam(
            Map<Integer, String> names, Map<String, Long> defaults, int id, String name, long fallback) {
        names.put(id, name);
        defaults.put(name, fallback);
    }

    static final Map<String, Long> CONFIG_DEFAULTS;

    static final String ACK_TIMEOUT_MS = "ack_timeout_ms";
    static final String AUTH_TIMEOUT_MS = "auth_timeout_ms";

    static {
        Map<String, Long> defaults = new LinkedHashMap<>();
        defaults.put(ACK_TIMEOUT_MS, 5000L);
        defaults.put(AUTH_TIMEOUT_MS, 10000L);
        CONFIG_DEFAULTS = Collections.unmodifiableMap(defaults);
    }

    private static long defaultLimit(String name) {
        Long fallback = CONFIG_DEFAULTS.get(name);
        if (fallback == null) {
            throw new IllegalArgumentException("알 수 없는 config 키: " + name);
        }
        return fallback;
    }

    static long configLimit(JsonObject setup, String name) {
        return setup.has("config") && setup.getAsJsonObject("config").has(name)
                ? setup.getAsJsonObject("config").get(name).getAsLong()
                : defaultLimit(name);
    }

    static final List<String> STATUS_NAMES = Arrays.asList(
            "OK",
            "CANCELLED",
            "UNKNOWN",
            "INVALID_ARGUMENT",
            "DEADLINE_EXCEEDED",
            "NOT_FOUND",
            "ALREADY_EXISTS",
            "PERMISSION_DENIED",
            "RESOURCE_EXHAUSTED",
            "FAILED_PRECONDITION",
            "ABORTED",
            "OUT_OF_RANGE",
            "UNIMPLEMENTED",
            "INTERNAL",
            "UNAVAILABLE",
            "DATA_LOSS",
            "UNAUTHENTICATED");

    static final List<String> ERROR_CODE_NAMES = Arrays.asList(
            "NO_ERROR",
            "PROTOCOL_ERROR",
            "INTERNAL_ERROR",
            "REFUSED",
            "CANCEL",
            "EXCESSIVE_USE",
            "UNRESPONSIVE",
            "UNAUTHENTICATED");

    static Map<String, Long> helloParams(JsonObject given) {
        Map<String, Long> params = new LinkedHashMap<>(HELLO_DEFAULTS);
        if (given != null) {
            for (String name : given.keySet()) {
                params.put(name, given.get(name).getAsLong());
            }
        }
        return params;
    }

    static long streamId(JsonElement element) {
        assertThat(element.getAsJsonPrimitive().isString())
                .as("스트림 번호는 문자열이다: %s", element)
                .isTrue();
        return Long.parseLong(element.getAsString());
    }

    static int errorCode(JsonElement element) {
        return element.getAsJsonPrimitive().isString()
                ? ERROR_CODE_NAMES.indexOf(element.getAsString())
                : element.getAsInt();
    }

    static String nameOfError(JsonElement element) {
        return nameOfError(errorCode(element));
    }

    static String nameOfError(int code) {
        return code < ERROR_CODE_NAMES.size() ? ERROR_CODE_NAMES.get(code) : String.valueOf(code);
    }

    static int statusCode(JsonElement element) {
        return element.getAsJsonPrimitive().isString()
                ? STATUS_NAMES.indexOf(element.getAsString())
                : element.getAsInt();
    }

    static String nameOfStatus(JsonElement element) {
        return element.getAsJsonPrimitive().isString() ? element.getAsString() : nameOfStatus(element.getAsInt());
    }

    static String nameOfStatus(int status) {
        return status < STATUS_NAMES.size() ? STATUS_NAMES.get(status) : String.valueOf(status);
    }
}
