package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.core.internal.Event;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** 통합 테스트가 함께 쓰는 값. */
final class Fixtures {

    /** 규격 4.1절: 애플리케이션 type은 256 이상이다. */
    static final int APPLICATION_TYPE = 256;

    /** 규격 2.5.1절 status {@code OK}. */
    static final int STATUS_OK = 0;

    /** 규격 2.5.1절 status {@code UNAVAILABLE}. */
    static final int STATUS_UNAVAILABLE = 14;

    /** 규격 2.5.2절 error_code {@code NO_ERROR}. */
    static final int NO_ERROR = 0;

    /** 규격 2.1절, 3.5절: CLOSE 프레임은 헤더 20바이트에 {@code error_code} 4바이트가 붙은 24바이트다(확장 영역과 {@code reason}이 없을 때). */
    static final int CLOSE_FRAME_BYTES = 24;

    private Fixtures() {}

    static List<Event.Kind> kinds(List<Event> events) {
        List<Event.Kind> kinds = new ArrayList<>();
        for (Event event : events) {
            kinds.add(event.kind());
        }
        return kinds;
    }

    static byte[] payload(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
