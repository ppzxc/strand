package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.api.ResourceLimits;
import io.github.ppzxc.strand.core.internal.Endpoint;
import org.jspecify.annotations.Nullable;

/**
 * 연결 하나가 진행하려면 꼭 있어야 하는 공간의 최소치. {@code ResourceLimits.requireProgress}에 넘기는 값과 연결을 만들 때 소유 객체 전체
 * 예산에서 예약하는 양이 여기서 나온다. 임의 수치가 아니라 core의 사건·출력 예약 상한({@link Endpoint#eventsNeeded}, {@link
 * Endpoint#outputsNeeded})과 선택한 TLS 엔진의 버퍼 크기({@link TlsSizes})에서 도출한다.
 *
 * <ul>
 *   <li>프레임 조립: core는 완성하지 못한 입력을 한 프레임까지 보관한다. 받을 수 있는 최대 프레임 크기다.
 *   <li>제어 출력: core가 한 단위에서 쌓을 수 있는 출력의 최대 개수({@link Endpoint#outputsNeeded})에 제어 프레임 하나의 크기({@value
 *       #CONTROL_OUTPUT_UNIT}, 헤더와 CLOSE의 고정 payload)를 곱한다. 열린 내 요청은 연결별 송신 요청 수 한도가, 응답을 기다리는 상대 요청은
 *       협상하는 동시 스트림 수(받는 요청 수 한도)가 막는다. 미뤄 둔 PING은 앱 PING이 없어 0이다.
 *   <li>결과 사건: 한 단위가 만드는 사건 수({@link Endpoint#eventsNeeded}) 가운데 응답 본문을 실을 수 있는 것은 응답 프레임 하나이므로 최대
 *       프레임 하나, 나머지는 본문 없는 로컬 결과로 프레임 헤더({@value Endpoint#FRAME_HEADER_SIZE}바이트) 하나 크기로 센다.
 *   <li>TLS 평문: 복호한 레코드 하나({@code getApplicationBufferSize}). 암호문: 받는 쪽 미완성 레코드의 나머지 한 패킷과 한 번에 읽는
 *       조각 한 패킷, 보내는 쪽 데이터 한 패킷({@code getPacketBufferSize} 셋, {@link TlsPassage}의 구성). TLS 제어: 핸드셰이크·경고를 쌓는 패킷
 *       하나. 이 셋은 서로 다른 저장 공간이라 모두 센다.
 * </ul>
 *
 * TLS를 쓰지 않는 연결(채널 보안을 TCP 밖에서 보장)은 TLS 공간을 예약하지 않는다. 다만 {@code requireProgress}가 모든 최소치가 양수일 것을
 * 요구하므로 그 검사에는 1을 넘긴다.
 */
final class ProgressSpace {
    /** 제어 프레임 하나의 크기: 프레임 헤더와 CLOSE의 고정 payload(error_code와 last_stream_id). 우리가 쌓는 가장 큰 제어 프레임이다. */
    static final long CONTROL_OUTPUT_UNIT = Endpoint.FRAME_HEADER_SIZE + Integer.BYTES + Long.BYTES;

    private final long frameAssembly;
    private final long controlOutput;
    private final long resultEvent;
    private final long tlsPlaintext;
    private final long tlsCiphertext;
    private final long tlsControl;

    private ProgressSpace(
            long frameAssembly,
            long controlOutput,
            long resultEvent,
            long tlsPlaintext,
            long tlsCiphertext,
            long tlsControl) {
        this.frameAssembly = frameAssembly;
        this.controlOutput = controlOutput;
        this.resultEvent = resultEvent;
        this.tlsPlaintext = tlsPlaintext;
        this.tlsCiphertext = tlsCiphertext;
        this.tlsControl = tlsControl;
    }

    /**
     * 최소치를 도출한다.
     *
     * @param outgoingRequests 연결별 송신 요청 수 한도
     * @param incomingRequests 연결별 받는 요청 수 한도(협상하는 동시 스트림 수)
     * @param maxFrameSize 받을 수 있는 최대 프레임 크기
     * @param tls TLS 엔진 크기. TLS를 쓰지 않으면 null
     */
    static ProgressSpace derive(
            long outgoingRequests, long incomingRequests, long maxFrameSize, @Nullable TlsSizes tls) {
        long control =
                Math.multiplyExact(Endpoint.outputsNeeded(outgoingRequests, 0, incomingRequests), CONTROL_OUTPUT_UNIT);
        long result = Math.addExact(maxFrameSize, Math.multiplyExact(Math.addExact(outgoingRequests, 1), (long)
                Endpoint.FRAME_HEADER_SIZE));
        if (tls == null) {
            return new ProgressSpace(maxFrameSize, control, result, 0, 0, 0);
        }
        return new ProgressSpace(
                maxFrameSize,
                control,
                result,
                tls.applicationBufferSize(),
                3L * tls.packetBufferSize(),
                tls.packetBufferSize());
    }

    long frameAssembly() {
        return frameAssembly;
    }

    long controlOutput() {
        return controlOutput;
    }

    long resultEvent() {
        return resultEvent;
    }

    long tlsPlaintext() {
        return tlsPlaintext;
    }

    long tlsCiphertext() {
        return tlsCiphertext;
    }

    long tlsControl() {
        return tlsControl;
    }

    /** {@code requireProgress}에 넘기는 최소치: 프레임, 제어, 결과, TLS 평문, 암호문, TLS 제어 순서다. TLS를 쓰지 않으면 TLS 최소치는 1이다. */
    long[] minimums() {
        return new long[] {
            frameAssembly,
            controlOutput,
            resultEvent,
            Math.max(1, tlsPlaintext),
            Math.max(1, tlsCiphertext),
            Math.max(1, tlsControl)
        };
    }

    /**
     * 두 범위의 한도가 최소치를 담는지 검사한다.
     *
     * @throws IllegalArgumentException 어느 범위든 한 종류라도 모자랄 때
     */
    void require(ResourceLimits limits) {
        long[] minimum = minimums();
        limits.requireProgress(minimum[0], minimum[1], minimum[2], minimum[3], minimum[4], minimum[5]);
    }

    /** 결과 사건 공간(바이트)이 담는 사건 수. 첫 칸은 최대 프레임 하나이고 나머지는 헤더 하나 크기다. 모자라도 한 칸은 준다. */
    static int eventSlots(long resultEventBytes, long maxFrameSize) {
        long slots = 1 + Math.max(0, resultEventBytes - maxFrameSize) / Endpoint.FRAME_HEADER_SIZE;
        return (int) Math.min(Integer.MAX_VALUE, slots);
    }

    /** 제어 출력 공간(바이트)이 담는 출력 수. */
    static int outputSlots(long controlOutputBytes) {
        return (int) Math.min(Integer.MAX_VALUE, controlOutputBytes / CONTROL_OUTPUT_UNIT);
    }
}
