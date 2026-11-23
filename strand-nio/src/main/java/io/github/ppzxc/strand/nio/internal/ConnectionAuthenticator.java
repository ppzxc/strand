package io.github.ppzxc.strand.nio.internal;

import io.github.ppzxc.strand.core.internal.AuthDecision;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * 연결 드라이버가 받은 {@code AUTH}를 애플리케이션에 맡기는 경계(규격 3.3.1절). core는 판단을 사건으로 위임하므로 드라이버는 전송 스레드에서 이 함수를
 * 부르되 판단이 끝나길 기다리지 않는다. 판단이 나오면 전송 스레드가 core에 입력한다. 함수는 전송 스레드에서 불리므로 블로킹하면 안 되고, 블로킹하는
 * 조회는 판단만 늦게 완료하는 {@link CompletionStage}로 돌려준다.
 */
@FunctionalInterface
public interface ConnectionAuthenticator {

    /**
     * 받은 {@code AUTH}를 판단한다.
     *
     * @param mechanism 메커니즘 이름
     * @param credentials 메커니즘이 정한 불투명 자격 증명. 비어 있을 수 있다
     * @return 판단. 던지거나 실패로 끝나면 일시 실패로 본다
     */
    CompletionStage<AuthDecision> authenticate(String mechanism, byte[] credentials);

    /**
     * 익명(자격 증명 없음)만 통과시키는 기본 인증기.
     *
     * @return 익명만 통과시키는 인증기
     */
    static ConnectionAuthenticator anonymousOnly() {
        return (mechanism, credentials) -> CompletableFuture.completedFuture(
                EndpointConfig.ANONYMOUS.equals(mechanism) && credentials.length == 0
                        ? AuthDecision.PASS
                        : AuthDecision.REJECT);
    }
}
