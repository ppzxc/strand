package io.github.ppzxc.strand.integration;

import java.nio.charset.StandardCharsets;

/** 통합 테스트가 함께 쓰는 값. */
final class Fixtures {

    /** 이벤트나 종료를 기다리는 상한. 넘기면 실패시켜 테스트가 멈추지 않게 한다. */
    static final long TIMEOUT_MS = 5000L;

    /** 규격 4.1절: 애플리케이션 type은 256 이상이다. */
    static final int APPLICATION_TYPE = 256;

    /** 규격 2.3절: PING의 type. */
    static final int PING_TYPE = 2;

    /** 규격 2.3절: CLOSE의 type. */
    static final int CLOSE_TYPE = 3;

    /** 규격 2.5.1절 status {@code OK}. */
    static final int STATUS_OK = 0;

    /** 규격 2.5.1절 status {@code UNAVAILABLE}. */
    static final int STATUS_UNAVAILABLE = 14;

    /** 규격 2.1절, 3.5절: CLOSE 프레임은 헤더 20바이트에 {@code error_code} 4바이트와 {@code last_stream_id} 8바이트가 붙은 32바이트다(확장 영역과 {@code reason}이 없을 때). */
    static final int CLOSE_FRAME_BYTES = 32;

    private Fixtures() {}

    static byte[] payload(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
