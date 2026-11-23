package io.github.ppzxc.strand.core.internal;

/** 엔드포인트가 상대에게 보내는 메시지 하나. 프레임 하나(연결 서문 포함)를 실은 바이트다. */
public final class OutboundMessage {

    private final byte[] bytes;

    private OutboundMessage(byte[] bytes) {
        this.bytes = bytes;
    }

    /** 프레임 하나를 실은 메시지. 애플리케이션 프레임인지는 프레임의 type으로 정한다(8바이트 연결 서문은 프레임이 아니다). */
    static OutboundMessage binary(byte[] bytes) {
        return new OutboundMessage(bytes);
    }

    /** 애플리케이션 프레임(요청·응답)이다. 8바이트 연결 서문은 프레임이 아니라 제어 쪽이다. 제어 프레임이 앞지를 수 있는 쪽이다(규격 3.6절). */
    boolean isApplication() {
        return bytes.length >= Frames.HEADER_SIZE && Frames.typeOf(bytes, 0) >= FrameType.FIRST_APPLICATION;
    }

    /**
     * 애플리케이션 프레임이면 그 프레임의 {@code stream_id}, 아니면 0이다(제어 프레임과 연결 서문). 복사하지 않는다. 전송 모듈이 와이어 배치를
     * 알지 않고도 쓰고 있는 출력이 어느 스트림의 요청·응답인지 알게 한다.
     */
    long applicationStreamId() {
        return isApplication() ? Frames.streamIdOf(bytes, 0) : 0;
    }

    /**
     * 메시지 내용.
     *
     * @return 메시지 내용
     */
    public byte[] bytes() {
        return bytes.clone();
    }

    /**
     * 메시지 내용의 길이(바이트). 복사하지 않는다.
     *
     * @return 내용 길이
     */
    public int length() {
        return bytes.length;
    }
}
