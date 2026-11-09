package io.github.ppzxc.strand.core.internal;

import org.jspecify.annotations.Nullable;

/** 확인 대기 하나. {@code payload}는 PING이면 보낸 8바이트, 아니면 null이다. 기한은 프레임을 전송 계층에 넘긴 때부터 센다. */
final class AckWait {
    /** 무엇의 ACK를 기다리는가. */
    enum Kind {
        HELLO,
        AUTH,
        PING
    }

    final Kind kind;
    final byte @Nullable [] payload;
    boolean started;
    long deadline;

    AckWait(Kind kind, byte @Nullable [] payload) {
        this.kind = kind;
        this.payload = payload;
    }

    void start(long deadline) {
        this.started = true;
        this.deadline = deadline;
    }
}
