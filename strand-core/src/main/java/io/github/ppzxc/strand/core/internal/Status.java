package io.github.ppzxc.strand.core.internal;

/**
 * 규격 2.5.1절의 {@code status} 값(애플리케이션 결과)과 error_code·수신 status를 status로 바꾸는 로컬 규칙({@link #forReset}, {@link
 * #normalize}). {@link ErrorCode}와 번호 공간이 다르다.
 */
public final class Status {

    public static final int OK = 0;

    public static final int CANCELLED = 1;
    /** 예약된 status는 이 값으로 취급한다. */
    public static final int UNKNOWN = 2;

    public static final int INVALID_ARGUMENT = 3;
    /** 요청 기한이 지났다(규격 4.4.1절). */
    public static final int DEADLINE_EXCEEDED = 4;

    public static final int UNIMPLEMENTED = 12;
    public static final int INTERNAL = 13;
    public static final int UNAVAILABLE = 14;
    /** 규격 2.5.1절에 정의된 마지막 status. */
    public static final int UNAUTHENTICATED = 16;

    private Status() {}

    /** status가 규격 2.5.1절의 보낼 수 있는 값(0~16)인가. 예약 값(17~255)은 보내지 않는다. */
    public static boolean isValid(int status) {
        return status >= OK && status <= UNAUTHENTICATED;
    }

    /** 내 요청이 받은 RESET으로 끝났을 때 애플리케이션에 넘기는 로컬 status(규격 4.4절). 예약 error_code는 INTERNAL_ERROR로 본다. */
    static int forReset(long errorCode) {
        if (errorCode == ErrorCode.CANCEL) {
            return CANCELLED;
        }
        return errorCode == ErrorCode.REFUSED ? UNAVAILABLE : INTERNAL;
    }

    /** 받은 status를 애플리케이션에 넘길 값으로 바꾼다. 예약된 값(17~255)은 {@code UNKNOWN}으로 취급한다(규격 2.5.1절). */
    static int normalize(int status) {
        return status > UNAUTHENTICATED ? UNKNOWN : status;
    }
}
