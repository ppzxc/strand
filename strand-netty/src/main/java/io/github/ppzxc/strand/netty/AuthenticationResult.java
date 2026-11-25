package io.github.ppzxc.strand.netty;

import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.util.Objects;
import java.util.OptionalLong;
import org.jspecify.annotations.Nullable;

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

    public static AuthenticationResult accepted(Object principal) {
        return new AuthenticationResult(
                Kind.ACCEPTED, Objects.requireNonNull(principal, "principal"), OptionalLong.empty());
    }

    public static AuthenticationResult accepted(Object principal, long lifetimeSeconds) {
        if (!EndpointConfig.isUnsigned32(lifetimeSeconds)) {
            throw new IllegalArgumentException("수명은 부호 없는 32비트 정수여야 한다: " + lifetimeSeconds);
        }
        return new AuthenticationResult(
                Kind.ACCEPTED, Objects.requireNonNull(principal, "principal"), OptionalLong.of(lifetimeSeconds));
    }

    public static AuthenticationResult rejected() {
        return new AuthenticationResult(Kind.REJECTED, null, OptionalLong.empty());
    }

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
