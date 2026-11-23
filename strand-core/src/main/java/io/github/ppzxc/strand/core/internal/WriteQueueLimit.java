package io.github.ppzxc.strand.core.internal;

/**
 * 전송 모듈이 읽기를 멈출 기준인 쓰기 대기열 상한(바이트). 연결 드라이버와 두 제품 엔진이 같은 셈을 쓰도록 어느 한쪽 전송 모듈이 아니라 core에 둔다.
 * core는 이 값으로 아무것도 하지 않는다.
 */
public final class WriteQueueLimit {

    /** 절대 한도의 기본값(64MiB). 상대가 HELLO로 알린 큰 값이 상한을 사실상 없애지 못하게 한다. */
    public static final long DEFAULT_ABSOLUTE_LIMIT = 64L * 1024 * 1024;

    private WriteQueueLimit() {}

    /**
     * 쓰기 대기열 상한을 셈한다. 규칙을 지키는 상대와의 정상 흐름 제어 범위다. 내 요청(상대가 알린 동시 스트림 × 상대가 알린 최대 프레임)과 상대 요청에
     * 대한 응답(내가 알린 동시 스트림 × 상대가 알린 최대 프레임), 거기에 제어 프레임 몇 개를 위한 프레임 하나를 더한다. 상대가 알린 값은 상대가
     * 정하므로 절대 한도로 막는다. 절대 한도가 작아도 상한은 상대가 알린 최대 프레임 하나보다 작아지지 않는다.
     *
     * @param peerMaxFrameSize 상대가 알린 최대 프레임 크기
     * @param peerMaxConcurrentStreams 상대가 알린 동시 스트림 수
     * @param ownMaxConcurrentStreams 내가 알린 동시 스트림 수
     * @param absoluteLimit 절대 한도
     * @return 상한(바이트)
     */
    public static long of(
            long peerMaxFrameSize, long peerMaxConcurrentStreams, long ownMaxConcurrentStreams, long absoluteLimit) {
        long range = (peerMaxConcurrentStreams + ownMaxConcurrentStreams) * peerMaxFrameSize + peerMaxFrameSize;
        return Math.max(peerMaxFrameSize, Math.min(range, absoluteLimit));
    }
}
