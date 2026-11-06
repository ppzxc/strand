package io.github.ppzxc.strand.integration;

import java.nio.charset.StandardCharsets;

/** 통합 테스트가 함께 쓰는 값. */
final class Fixtures {

    /** 규격 4.1절: 애플리케이션 type은 256 이상이다. */
    static final int APPLICATION_TYPE = 256;

    /** 규격 2.5.1절 status {@code OK}. */
    static final int STATUS_OK = 0;

    /** 규격 2.5.2절 error_code {@code NO_ERROR}. */
    static final int NO_ERROR = 0;

    private Fixtures() {}

    static byte[] payload(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
