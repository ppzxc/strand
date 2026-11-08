package io.github.ppzxc.strand.core.internal;

/** 서버의 애플리케이션이 받은 {@code AUTH}에 내리는 판단(규격 3.3.1절). */
public enum AuthDecision {
    /** 자격 증명을 통과시킨다. 서버는 {@code AUTH(ACK)}를 보낸다. */
    PASS,
    /** 자격 증명을 거부한다. 서버는 {@code CLOSE(UNAUTHENTICATED)}를 보내야 한다. 아직 동작하지 않는다. */
    REJECT,
    /** 인증 서버 장애 같은 일시적 실패다. 서버는 {@code CLOSE(INTERNAL_ERROR)}를 보내야 한다. 아직 동작하지 않는다. */
    TRANSIENT_FAILURE
}
