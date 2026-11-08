package io.github.ppzxc.strand.core.internal;

/** 서버가 받은 {@code AUTH}의 메커니즘과 자격 증명을 애플리케이션에 넘기는 경계(규격 3.3.1절). 코어는 메커니즘을 구현하지 않는다. */
@FunctionalInterface
public interface Authenticator {

    /**
     * 받은 {@code AUTH}를 판단한다.
     *
     * @param mechanism 메커니즘 이름
     * @param credentials 메커니즘이 정한 불투명 자격 증명. 비어 있을 수 있다
     * @return 판단
     */
    AuthDecision authenticate(String mechanism, byte[] credentials);
}
