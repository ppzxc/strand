package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonObject;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 하네스가 쓰는 프로토콜 상수의 유일한 정의. 러너, 확인 대기 해석기, 필드 규칙이 모두 여기서 가져다 쓴다. 하네스는 구현과 독립이어야
 * 하므로 core의 상수를 쓰지 않고 규격의 값을 따로 둔다.
 */
final class Protocol {

    private Protocol() {}

    // ---- 연결 서문(규격 3.1절) ----

    /** 연결 서문의 식별자 6바이트(hex). 뒤의 2바이트는 예약이라 값을 보지 않는다. */
    static final String PREFACE_PREFIX_HEX = "c0de53545230";
    /** 이 하네스가 보내는 연결 서문 8바이트(hex). 예약 2바이트는 0이다. */
    static final String PREFACE_HEX = PREFACE_PREFIX_HEX + "0000";

    // ---- 헤더 배치(규격 2.1절): 바이트 위치와 그 hex 위치 ----

    static final int HEADER_SIZE = 20;
    static final int HEADER_HEX = HEADER_SIZE * 2;
    /** 맨 앞 {@code length} 필드의 크기. */
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
    /** 프레임 {@code length}의 최솟값(규격 2.2.1절). */
    static final int MIN_FRAME_LENGTH = 16;
    /** CLOSE payload의 최소 크기(error_code 4바이트와 last_stream_id 8바이트)와 RESET payload의 크기. */
    static final int CLOSE_PAYLOAD_SIZE = 12;

    static final int RESET_PAYLOAD_SIZE = 4;
    /** PING payload의 크기(규격 3.3절). */
    static final int PING_PAYLOAD_SIZE = 8;

    // ---- 플래그와 프레임 타입 ----

    static final int ACK_FLAG = 0x01;
    static final int TYPE_HELLO = 0x0001;
    static final int TYPE_PING = 0x0002;
    static final int TYPE_CLOSE = 0x0003;
    static final int TYPE_RESET = 0x0004;
    static final int TYPE_AUTH = 0x0005;
    /** 0~255는 제어 프레임용으로 예약돼 있다. */
    static final int FIRST_APPLICATION_TYPE = 256;
    /** WebSocket Close의 상태 코드 가운데 규격이 정한 {@code 1000 Normal Closure}(규격 1.2절). */
    static final int NORMAL_CLOSURE = 1000;

    static final String DEFAULT_AUTH_MECHANISM = "ANONYMOUS";

    // ---- HELLO 파라미터(규격 3.2절): 번호와 이름, 기본값 ----

    /** 파라미터 번호에서 이름으로. 아래 표 하나에서 나온다. */
    static final Map<Integer, String> HELLO_PARAM_NAMES;
    /** 파라미터 이름에서 기본값으로. 형태 검사의 HELLO 파라미터 이름도 여기서 나온다. */
    static final Map<String, Long> HELLO_DEFAULTS;
    /** 파라미터 항목 하나의 크기: 번호 2바이트와 값 4바이트. */
    static final int PARAM_ENTRY_SIZE = 6;
    /** AUTH(ACK) 파라미터 중 이름이 있는 {@code credential_lifetime}의 번호. */
    static final int AUTH_PARAM_CREDENTIAL_LIFETIME = 1;

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

    // ---- 설정 키(벡터 형식 5절): config 키와 기본 시간 값(ms) ----

    /** {@code setup.config}의 키와 기본값. 이 목록이 형태 검사의 config 키와 러너의 지원 목록을 정한다. */
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

    /** {@code setup.config}에 정한 한도(ms). 없으면 기본값이다. */
    static long configLimit(JsonObject setup, String name) {
        return setup.has("config") && setup.getAsJsonObject("config").has(name)
                ? setup.getAsJsonObject("config").get(name).getAsLong()
                : defaultLimit(name);
    }

    /** 2.5.1절 status 이름, 코드 값 순서. 목록 크기가 곧 첫 예약 정수 값이다. */
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

    /** 2.5.2절 error_code 이름, 코드 값 순서. 목록 크기가 곧 첫 예약 정수 값이다. */
    static final List<String> ERROR_CODE_NAMES = Arrays.asList(
            "NO_ERROR",
            "PROTOCOL_ERROR",
            "INTERNAL_ERROR",
            "REFUSED",
            "CANCEL",
            "EXCESSIVE_USE",
            "UNRESPONSIVE",
            "UNAUTHENTICATED");
}
