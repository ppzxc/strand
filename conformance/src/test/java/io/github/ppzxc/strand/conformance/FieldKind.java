package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonElement;
import java.util.Arrays;
import java.util.List;

/** 필드 값의 종류. 종류마다 값을 검증하는 규칙을 하나씩 가진다. */
enum FieldKind {

    /** 객체 검증이 값을 보지 않는다. 호출한 쪽이 객체 검증 앞이나 뒤에 따로 검증한다. */
    DEFERRED {
        @Override
        void check(Check c, String path, JsonElement element) {}
    },
    ARRAY {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.array(path, element);
        }
    },
    /** 소문자 hex 바이트열. */
    HEX {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.hex(path, element);
        }
    },
    U16 {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.range(path, element, 0, U16_MAX);
        }
    },
    U32 {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.range(path, element, 0, U32_MAX);
        }
    },
    /** 1 이상의 u32. 0이 아닌 스트림 ID, 시간(ms), 보관 한도에 쓴다. */
    POSITIVE_U32 {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.range(path, element, 1, U32_MAX);
        }
    },
    /** 애플리케이션 프레임·요청의 type. 0~255는 제어 프레임용으로 예약돼 있다. */
    APP_TYPE {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.range(path, element, FIRST_APP_TYPE, U16_MAX);
        }
    },
    /** 2.5.1절의 이름이거나, 이름 목록 다음부터 255까지의 예약 정수. */
    STATUS {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.named(path, element, STATUS_NAMES, U8_MAX);
        }
    },
    /** 2.5.2절의 이름이거나, 이름 목록 다음부터 u32 끝까지의 예약 정수. */
    ERROR_CODE {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.named(path, element, ERROR_CODE_NAMES, U32_MAX);
        }
    },
    EXTENSIONS {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.extensions(path, element);
        }
    },
    HELLO_PARAMS {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.object(path, element, VectorSchema.HELLO_PARAMS);
        }
    };

    static final long U8_MAX = 255L;
    static final long U16_MAX = 65535L;
    static final long U32_MAX = 4294967295L;
    static final long FIRST_APP_TYPE = 256L;

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
            "FRAME_SIZE_ERROR",
            "HELLO_TIMEOUT",
            "STREAM_CLOSED",
            "REFUSED",
            "CANCEL",
            "OVERLOAD");

    abstract void check(Check c, String path, JsonElement element);
}
