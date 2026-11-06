package io.github.ppzxc.strand.core.internal;

/** 엔드포인트가 보내는 HELLO의 파라미터(규격 3.2.1절). 값을 바꾼 복사본을 돌려주는 불변 객체다. */
public final class EndpointConfig {

    private static final long DEFAULT_MAX_FRAME_SIZE = 4_194_304L;
    private static final long DEFAULT_MAX_CONCURRENT_STREAMS = 100L;

    private final long maxFrameSize;
    private final long maxConcurrentStreams;
    private final long capabilities;

    private EndpointConfig(long maxFrameSize, long maxConcurrentStreams, long capabilities) {
        this.maxFrameSize = maxFrameSize;
        this.maxConcurrentStreams = maxConcurrentStreams;
        this.capabilities = capabilities;
    }

    /**
     * 규격 기본값의 설정.
     *
     * @return 기본 설정
     */
    public static EndpointConfig defaults() {
        return new EndpointConfig(DEFAULT_MAX_FRAME_SIZE, DEFAULT_MAX_CONCURRENT_STREAMS, 0L);
    }

    /**
     * 받을 수 있는 프레임의 최대 크기를 바꾼 복사본.
     *
     * @param value 바이트 수
     * @return 새 설정
     */
    public EndpointConfig maxFrameSize(long value) {
        return new EndpointConfig(value, maxConcurrentStreams, capabilities);
    }

    /**
     * 상대가 동시에 열 수 있는 스트림 수를 바꾼 복사본.
     *
     * @param value 스트림 수
     * @return 새 설정
     */
    public EndpointConfig maxConcurrentStreams(long value) {
        return new EndpointConfig(maxFrameSize, value, capabilities);
    }

    /**
     * 기능 비트셋을 바꾼 복사본.
     *
     * @param value 비트셋
     * @return 새 설정
     */
    public EndpointConfig capabilities(long value) {
        return new EndpointConfig(maxFrameSize, maxConcurrentStreams, value);
    }

    long maxFrameSize() {
        return maxFrameSize;
    }

    long maxConcurrentStreams() {
        return maxConcurrentStreams;
    }

    long capabilities() {
        return capabilities;
    }
}
