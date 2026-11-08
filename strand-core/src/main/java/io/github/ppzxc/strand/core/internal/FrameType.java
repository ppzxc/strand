package io.github.ppzxc.strand.core.internal;

/** 규격 2.2.2절의 프레임 {@code type} 값. */
final class FrameType {

    static final int HELLO = 0x0001;
    static final int PING = 0x0002;
    static final int CLOSE = 0x0003;
    static final int RESET = 0x0004;
    static final int AUTH = 0x0005;
    /** 이 값부터 애플리케이션 프레임이다(규격 2.2.2절). */
    static final int FIRST_APPLICATION = 0x0100;

    private FrameType() {}

    /** 예약된 제어 타입: {@code 0x0000}과 {@code 0x0006}~{@code 0x00FF}. 받으면 무시한다(규격 2.2.2절). {@link #AUTH}는 예약이 아니다. */
    static boolean isReserved(int type) {
        return type == 0 || (type > AUTH && type < FIRST_APPLICATION);
    }
}
