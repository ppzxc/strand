package io.github.ppzxc.strand.core.internal;

/** 규격 2.2.2절의 프레임 {@code type} 값. */
final class FrameType {

    static final int HELLO = 0x0001;
    static final int PING = 0x0002;
    static final int CLOSE = 0x0003;
    static final int RESET = 0x0004;
    /** 이 값부터 애플리케이션 프레임이다(규격 2.2.2절). */
    static final int FIRST_APPLICATION = 0x0100;

    private FrameType() {}
}
