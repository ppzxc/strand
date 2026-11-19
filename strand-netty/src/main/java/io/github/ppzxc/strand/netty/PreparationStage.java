package io.github.ppzxc.strand.netty;

/** 연결 준비가 실패한 단계. */
public enum PreparationStage {
    /** 주소 해석·TCP 연결 또는 접수. */
    CONNECTION,
    /** 채널 보안(TLS 핸드셰이크, ALPN, 호스트 이름 검증). */
    SECURITY,
    /** 규격 3.2절 연결 서문과 HELLO. */
    HELLO,
    /** 연결 인증(AUTH). */
    AUTHENTICATION,
    /** 핸들러 팩터리. */
    HANDLER_FACTORY
}
