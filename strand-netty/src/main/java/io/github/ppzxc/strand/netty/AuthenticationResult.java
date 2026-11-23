package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.util.Objects;
import java.util.OptionalLong;
import org.jspecify.annotations.Nullable;

/** {@link CredentialVerifier}의 판단. */
public final class AuthenticationResult {
    enum Kind {
        ACCEPTED,
        REJECTED,
        UNAVAILABLE
    }

    private final Kind kind;
    private final @Nullable Object principal;
    private final OptionalLong lifetime;

    private AuthenticationResult(Kind kind, @Nullable Object principal, OptionalLong lifetime) {
        this.kind = kind;
        this.principal = principal;
        this.lifetime = lifetime;
    }

    /**
     * 자격 증명을 통과시킨다.
     *
     * @param principal 인증된 주체. 연결의 {@link NettyStrandConnection#authenticatedPeer()}로 조회된다
     * @return 판단
     */
    public static AuthenticationResult accepted(Object principal) {
        return new AuthenticationResult(
                Kind.ACCEPTED, Objects.requireNonNull(principal, "principal"), OptionalLong.empty());
    }

    /**
     * 자격 증명을 통과시키고 남은 자격 증명 수명을 상대에게 알린다. 수명은 {@code AUTH(ACK)}의 {@code credential_lifetime}으로 실리고, 클라이언트는
     * 연결에서 읽는다. 라이브러리는 수명이 지나도 연결을 스스로 닫지 않는다. 닫는 판단은 애플리케이션이 한다.
     *
     * @param principal 인증된 주체
     * @param lifetimeSeconds 남은 수명(초). 부호 없는 32비트 범위(0 이상 4294967295 이하)여야 한다
     * @return 판단
     * @throws IllegalArgumentException 수명이 범위를 벗어났을 때
     */
    public static AuthenticationResult accepted(Object principal, long lifetimeSeconds) {
        if (!EndpointConfig.isUnsigned32(lifetimeSeconds)) {
            throw new IllegalArgumentException("수명은 부호 없는 32비트 정수여야 한다: " + lifetimeSeconds);
        }
        return new AuthenticationResult(
                Kind.ACCEPTED, Objects.requireNonNull(principal, "principal"), OptionalLong.of(lifetimeSeconds));
    }

    /**
     * 자격 증명을 거부한다. 연결은 {@code CLOSE(UNAUTHENTICATED)}로 닫힌다.
     *
     * @return 판단
     */
    public static AuthenticationResult rejected() {
        return new AuthenticationResult(Kind.REJECTED, null, OptionalLong.empty());
    }

    /**
     * 인증 서버 장애 같은 일시적 실패를 알린다. 연결은 {@code CLOSE(INTERNAL_ERROR)}로 닫힌다.
     *
     * @return 판단
     */
    public static AuthenticationResult unavailable() {
        return new AuthenticationResult(Kind.UNAVAILABLE, null, OptionalLong.empty());
    }

    Kind kind() {
        return kind;
    }

    @Nullable
    Object principal() {
        return principal;
    }

    OptionalLong lifetime() {
        return lifetime;
    }
}
