package io.github.ppzxc.strand.core.internal;

/** 전송 모듈이 쓰는 내부 출력의 식별자와 불변 내용. */
public final class EndpointOutput {
    private final long id;
    private final OutboundMessage message;

    EndpointOutput(long id, OutboundMessage message) {
        this.id = id;
        this.message = message;
    }

    public long id() {
        return id;
    }

    /** 출력의 종류와 내용. 불변이다. */
    public OutboundMessage message() {
        return message;
    }

    /**
     * 이 출력이 애플리케이션 프레임(요청·응답)이면 그 {@code stream_id}, 아니면(제어 프레임·연결 서문·WebSocket Close) 0이다. 복사하지 않는다.
     *
     * @return 스트림 번호 또는 0
     */
    public long applicationStreamId() {
        return message.applicationStreamId();
    }

    public byte[] bytes() {
        return message.bytes();
    }

    /** 복사하지 않는 읽기 전용 내용. 호출자는 배열을 바꾸면 안 된다({@link OutboundMessage#readOnlyBytes()}). */
    public byte[] readOnlyBytes() {
        return message.readOnlyBytes();
    }

    public int length() {
        return message.length();
    }
}
