package io.github.ppzxc.strand.core.internal;

import org.jspecify.annotations.Nullable;

/** 확인 대기 하나. {@code payload}는 PING이면 보낸 8바이트, HELLO이면 null이다. 기한은 프레임을 전송 계층에 넘긴 때부터 센다. */
final class AckWait {
    final byte @Nullable [] payload;
    boolean started;
    long deadline;

    AckWait(byte @Nullable [] payload) {
        this.payload = payload;
    }

    void start(long deadline) {
        this.started = true;
        this.deadline = deadline;
    }
}
