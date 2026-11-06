package io.github.ppzxc.strand.examples;

import io.github.ppzxc.strand.core.internal.Event;

/** 예제가 함께 쓰는 값과 출력 도우미. */
final class ExampleSupport {

    /** 규격 4.1절: 애플리케이션 type은 256 이상이다. 예제는 256을 쓴다. */
    static final int DEMO_REQUEST_TYPE = 256;

    /** 규격 2.5.1절 status {@code OK}. */
    static final int STATUS_OK = 0;

    /** 규격 2.5.2절 error_code {@code NO_ERROR}. */
    static final int NO_ERROR = 0;

    private ExampleSupport() {}

    static String describe(Event event) {
        switch (event.kind()) {
            case RESULT:
                return "RESULT stream=" + event.streamId() + " status=" + event.status();
            case REQUEST_RECEIVED:
                return "REQUEST_RECEIVED stream=" + event.streamId() + " type=" + event.type();
            default:
                return "CLOSED";
        }
    }

    static String arg(String[] args, int index, String fallback) {
        return args.length > index ? args[index] : fallback;
    }
}
