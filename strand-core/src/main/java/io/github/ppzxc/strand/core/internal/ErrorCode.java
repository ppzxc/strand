package io.github.ppzxc.strand.core.internal;

/** 규격 2.5.2절의 {@code error_code} 값(CLOSE·RESET). {@link Status}와 번호 공간이 다르다. */
public final class ErrorCode {

    public static final int NO_ERROR = 0;
    public static final int PROTOCOL_ERROR = 1;
    public static final int INTERNAL_ERROR = 2;
    public static final int REFUSED = 3;
    public static final int CANCEL = 4;
    public static final int EXCESSIVE_USE = 5;
    public static final int UNRESPONSIVE = 6;
    public static final int UNAUTHENTICATED = 7;
    /** 이 값부터 예약이다. */
    static final int RESERVED_FROM = 8;

    private ErrorCode() {}

    /**
     * CLOSE에 쓸 수 있는 error_code인가. RESET 전용 코드({@link #REFUSED}, {@link #CANCEL})와 예약 값은 쓸 수 없다.
     *
     * @param errorCode error_code 값
     * @return CLOSE에 쓸 수 있으면 true
     */
    public static boolean isCloseCode(int errorCode) {
        switch (errorCode) {
            case NO_ERROR:
            case PROTOCOL_ERROR:
            case INTERNAL_ERROR:
            case EXCESSIVE_USE:
            case UNRESPONSIVE:
            case UNAUTHENTICATED:
                return true;
            default:
                return false;
        }
    }

    /** 받은 CLOSE의 error_code를 종료 원인에 실을 값으로 바꾼다. 예약된 값(8 이상)은 {@link #INTERNAL_ERROR}로 취급한다(규격 2.5.2절). */
    static long normalizeReceived(long errorCode) {
        return errorCode >= RESERVED_FROM ? INTERNAL_ERROR : errorCode;
    }
}
