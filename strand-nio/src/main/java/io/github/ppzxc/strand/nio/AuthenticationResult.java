package io.github.ppzxc.strand.nio;

import java.util.Objects;
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

    private AuthenticationResult(Kind kind, @Nullable Object principal) {
        this.kind = kind;
        this.principal = principal;
    }

    /**
     * 자격 증명을 통과시킨다.
     *
     * @param principal 인증된 주체. 연결의 {@link NioStrandConnection#authenticatedPeer()}로 조회된다
     * @return 판단
     */
    public static AuthenticationResult accepted(Object principal) {
        return new AuthenticationResult(Kind.ACCEPTED, Objects.requireNonNull(principal, "principal"));
    }

    /**
     * 자격 증명을 거부한다. 연결은 {@code CLOSE(UNAUTHENTICATED)}로 닫힌다.
     *
     * @return 판단
     */
    public static AuthenticationResult rejected() {
        return new AuthenticationResult(Kind.REJECTED, null);
    }

    /**
     * 인증 서버 장애 같은 일시적 실패를 알린다. 연결은 {@code CLOSE(INTERNAL_ERROR)}로 닫힌다.
     *
     * @return 판단
     */
    public static AuthenticationResult unavailable() {
        return new AuthenticationResult(Kind.UNAVAILABLE, null);
    }

    Kind kind() {
        return kind;
    }

    @Nullable
    Object principal() {
        return principal;
    }
}
