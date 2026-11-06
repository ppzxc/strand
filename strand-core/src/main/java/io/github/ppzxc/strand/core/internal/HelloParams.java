package io.github.ppzxc.strand.core.internal;

/** 규격 3.2절의 HELLO 파라미터: identifier, 기본값, 허용 범위. */
final class HelloParams {

    /** 파라미터 하나의 크기: identifier(u16)와 value(u32). */
    static final int HELLO_PARAM_SIZE = 6;

    static final int MAX_FRAME_SIZE_ID = 0x0001;
    static final int MAX_CONCURRENT_STREAMS_ID = 0x0002;
    static final int CAPABILITIES_ID = 0x0003;

    static final long DEFAULT_MAX_FRAME_SIZE = 4_194_304L;
    static final long DEFAULT_MAX_CONCURRENT_STREAMS = 100L;
    static final long MAX_FRAME_SIZE_LOWER_LIMIT = 16384L;
    static final long MAX_FRAME_SIZE_UPPER_LIMIT = 16_777_216L;

    private HelloParams() {}
}
