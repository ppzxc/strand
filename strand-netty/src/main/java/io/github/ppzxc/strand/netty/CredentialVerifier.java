package io.github.ppzxc.strand.netty;

/** 서버가 받은 연결 인증(AUTH)을 판단한다. 앱용 실행기에서 불리므로 블로킹해도 전송 스레드를 막지 않는다. */
@FunctionalInterface
public interface CredentialVerifier {

    /**
     * 자격 증명을 판단한다. 던지면 일시적 실패로 보고 준비 실패의 원인으로 관찰된다.
     *
     * @param credentials 받은 메커니즘과 자격 증명
     * @return 판단
     * @throws Exception 판단하지 못했을 때
     */
    AuthenticationResult verify(Credentials credentials) throws Exception;
}
