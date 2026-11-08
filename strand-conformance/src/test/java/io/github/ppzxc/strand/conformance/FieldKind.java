package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonElement;
import java.math.BigInteger;
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
    /** 1 이상의 u32. 0이 아닌 스트림 ID와 시간(ms)에 쓴다. */
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
    /** 스트림 번호. 0 이상 2^63-1 이하의 10진수 문자열(번호는 JSON 수로 쓰지 않는다). */
    STREAM_ID {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.decimal(path, element, BigInteger.ZERO, STREAM_ID_MAX);
        }
    },
    /** CLOSE의 last_stream_id. 0 이상 2^64-1 이하의 10진수 문자열(형태가 잘못된 CLOSE를 위해 번호 범위 밖 값도 쓴다). */
    LAST_STREAM_ID {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.decimal(path, element, BigInteger.ZERO, U64_MAX);
        }
    },
    /** 1 이상 2^63-1 이하의 10진수 문자열. RESET의 stream_id와 next_stream_id에 쓴다(홀짝은 role과 함께 보는 검사라 {@link VectorSchema}가 따로 본다). */
    POSITIVE_STREAM_ID {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.decimal(path, element, BigInteger.ONE, STREAM_ID_MAX);
        }
    },
    /** 제어 프레임의 stream_id처럼 값이 0으로 고정된 필드. 문자열 {@code "0"}이다. */
    ZERO_STREAM_ID {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.decimal(path, element, BigInteger.ZERO, BigInteger.ZERO);
        }
    },
    /** WebSocket Close의 상태 코드. 규격 1.2절이 정한 {@code 1000 Normal Closure}만 모델링한다. */
    WEBSOCKET_CLOSE_CODE {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.range(path, element, 1000, 1000);
        }
    },
    /** HELLO·PING의 flags(ACK). */
    ACK_FLAGS {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.flags(path, element, VectorSchema.ACK_FLAGS);
        }
    },
    PING_PAYLOAD {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.pingPayload(path, element);
        }
    },
    /** 프레임 기대값의 배열. */
    FRAMES {
        @Override
        void check(Check c, String path, JsonElement element) {
            VectorSchema.frames(c, path, element);
        }
    },
    /** 관찰 이벤트 기대값의 배열. */
    EVENTS {
        @Override
        void check(Check c, String path, JsonElement element) {
            VectorSchema.events(c, path, element);
        }
    },
    BOOLEAN {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.bool(path, element);
        }
    },
    /** 결과 확정성(규격 1.3절): 응답 받음, 처리 안 됨, 결과 모름. */
    CERTAINTY {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.oneOf(path, element, VectorSchema.CERTAINTIES);
        }
    },
    ROLE {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.oneOf(path, element, VectorSchema.ROLES);
        }
    },
    /** 0xFFFF 이하 256 이상 정수의 집합(중복 불가). */
    KNOWN_TYPES {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.uniqueAppTypes(path, element);
        }
    },
    TRANSPORT {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.oneOf(path, element, VectorSchema.TRANSPORTS);
        }
    },
    OPCODE {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.oneOf(path, element, VectorSchema.OPCODES);
        }
    },
    SETUP_CONFIG {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.object(path, element, VectorSchema.SETUP_CONFIG);
        }
    },
    FORMAT {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.format(path, element, VectorSchema.KNOWN_FORMAT);
        }
    },
    VECTOR_ID {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.id(path, element);
        }
    },
    DESCRIPTION {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.nonEmptyText(path, element);
        }
    },
    SPEC_SECTIONS {
        @Override
        void check(Check c, String path, JsonElement element) {
            c.specSections(path, element);
        }
    },
    SETUP {
        @Override
        void check(Check c, String path, JsonElement element) {
            VectorSchema.setup(c, path, element);
        }
    },
    STEPS {
        @Override
        void check(Check c, String path, JsonElement element) {
            VectorSchema.steps(c, path, element);
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
    static final BigInteger U64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    static final BigInteger STREAM_ID_MAX = BigInteger.ONE.shiftLeft(63).subtract(BigInteger.ONE);

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

    abstract void check(Check c, String path, JsonElement element);
}
