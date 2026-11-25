package io.github.ppzxc.strand.core.internal;

final class HelloParams {

    static final int HELLO_PARAM_SIZE = 6;

    static final int MAX_FRAME_SIZE_ID = 0x0001;
    static final int MAX_CONCURRENT_STREAMS_ID = 0x0002;
    static final int CAPABILITIES_ID = 0x0003;

    static final long DEFAULT_MAX_FRAME_SIZE = 65_536L;
    static final long DEFAULT_MAX_CONCURRENT_STREAMS = 100L;
    static final long MAX_FRAME_SIZE_UPPER_LIMIT = 1_048_576L;

    private HelloParams() {}
}
