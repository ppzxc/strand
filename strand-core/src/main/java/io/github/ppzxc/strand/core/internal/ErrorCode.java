package io.github.ppzxc.strand.core.internal;

/** 규격 2.5.2절의 {@code error_code} 값(CLOSE·RESET). {@link Status}와 번호 공간이 다르다. */
final class ErrorCode {

    static final int NO_ERROR = 0;
    static final int PROTOCOL_ERROR = 1;
    static final int REFUSED = 3;
    static final int CANCEL = 4;

    private ErrorCode() {}
}
