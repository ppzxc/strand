package io.github.ppzxc.strand.core.internal;

import java.util.HashSet;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/** 엔드포인트의 설정: 보내는 HELLO의 파라미터(규격 3.2.1절)와 확인 대기 시간. 값을 바꾼 복사본을 돌려주는 불변 객체다. */
public final class EndpointConfig {

    private static final long DEFAULT_MAX_FRAME_SIZE = 4_194_304L;
    private static final long DEFAULT_MAX_CONCURRENT_STREAMS = 100L;
    private static final long DEFAULT_ACK_TIMEOUT_MS = 5000L;

    private final long maxFrameSize;
    private final long maxConcurrentStreams;
    private final long capabilities;
    private final long ackTimeoutMs;
    /** 구현이 아는 애플리케이션 {@code type}. null이면 모든 type을 안다(규격 4.1절). */
    private final @Nullable Set<Integer> knownTypes;
    /** 다음에 쓸 스트림 번호. null이면 역할의 첫 번호다(규격 2.2.5절). */
    private final @Nullable Long nextStreamId;

    private EndpointConfig(Values values) {
        this.maxFrameSize = values.maxFrameSize;
        this.maxConcurrentStreams = values.maxConcurrentStreams;
        this.capabilities = values.capabilities;
        this.ackTimeoutMs = values.ackTimeoutMs;
        this.knownTypes = values.knownTypes;
        this.nextStreamId = values.nextStreamId;
    }

    /** 설정 값의 모음. 복사본을 만들 때 한 필드만 고치는 데 쓴다. 필드를 더해도 생성자 인자 나열은 늘지 않는다. */
    private static final class Values {
        long maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
        long maxConcurrentStreams = DEFAULT_MAX_CONCURRENT_STREAMS;
        long capabilities;
        long ackTimeoutMs = DEFAULT_ACK_TIMEOUT_MS;

        @Nullable
        Set<Integer> knownTypes;

        @Nullable
        Long nextStreamId;
    }

    private Values values() {
        Values values = new Values();
        values.maxFrameSize = maxFrameSize;
        values.maxConcurrentStreams = maxConcurrentStreams;
        values.capabilities = capabilities;
        values.ackTimeoutMs = ackTimeoutMs;
        values.knownTypes = knownTypes;
        values.nextStreamId = nextStreamId;
        return values;
    }

    /**
     * 규격 기본값의 설정.
     *
     * @return 기본 설정
     */
    public static EndpointConfig defaults() {
        return new EndpointConfig(new Values());
    }

    /**
     * 받을 수 있는 프레임의 최대 크기를 바꾼 복사본.
     *
     * @param value 바이트 수
     * @return 새 설정
     */
    public EndpointConfig maxFrameSize(long value) {
        Values values = values();
        values.maxFrameSize = value;
        return new EndpointConfig(values);
    }

    /**
     * 상대가 동시에 열 수 있는 스트림 수를 바꾼 복사본.
     *
     * @param value 스트림 수
     * @return 새 설정
     */
    public EndpointConfig maxConcurrentStreams(long value) {
        Values values = values();
        values.maxConcurrentStreams = value;
        return new EndpointConfig(values);
    }

    /**
     * 기능 비트셋을 바꾼 복사본.
     *
     * @param value 비트셋
     * @return 새 설정
     */
    public EndpointConfig capabilities(long value) {
        Values values = values();
        values.capabilities = value;
        return new EndpointConfig(values);
    }

    /**
     * HELLO나 PING을 보내고 ACK를 기다리는 시간(확인 대기, 규격 3.3절)을 바꾼 복사본. 규격은 값을 정하지 않는다.
     *
     * @param value 밀리초
     * @return 새 설정
     */
    public EndpointConfig ackTimeoutMs(long value) {
        Values values = values();
        values.ackTimeoutMs = value;
        return new EndpointConfig(values);
    }

    /**
     * 구현이 아는 애플리케이션 type 목록을 정한 복사본. 정하지 않으면 모든 type을 안다. 모르는 type의 새 요청에는
     * {@code UNIMPLEMENTED}로 답한다(규격 4.1절).
     *
     * @param types 아는 type들
     * @return 새 설정
     */
    public EndpointConfig knownTypes(Set<Integer> types) {
        Values values = values();
        values.knownTypes = new HashSet<>(types);
        return new EndpointConfig(values);
    }

    /**
     * 내 번호 공간에서 다음에 쓸 스트림 번호를 정한 복사본. 번호가 소진되는 경우를 만들어 볼 때 쓴다. 정하지 않으면 클라이언트는 1,
     * 서버는 2에서 시작한다(규격 2.2.5절).
     *
     * @param value 다음 스트림 번호. 역할의 번호 공간(클라이언트는 홀수, 서버는 짝수)에 속해야 한다
     * @return 새 설정
     */
    public EndpointConfig nextStreamId(long value) {
        Values values = values();
        values.nextStreamId = value;
        return new EndpointConfig(values);
    }

    boolean knows(int type) {
        return knownTypes == null || knownTypes.contains(type);
    }

    long firstStreamId(long roleDefault) {
        return nextStreamId == null ? roleDefault : nextStreamId;
    }

    long ackTimeoutMs() {
        return ackTimeoutMs;
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
