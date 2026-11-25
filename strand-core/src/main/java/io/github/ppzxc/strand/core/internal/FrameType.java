package io.github.ppzxc.strand.core.internal;

final class FrameType {

    static final int HELLO = 0x0001;
    static final int PING = 0x0002;
    static final int CLOSE = 0x0003;
    static final int RESET = 0x0004;
    static final int AUTH = 0x0005;

    static final int FIRST_APPLICATION = 0x0100;

    private FrameType() {}

    static boolean isReserved(int type) {
        return type == 0 || (type > AUTH && type < FIRST_APPLICATION);
    }
}
