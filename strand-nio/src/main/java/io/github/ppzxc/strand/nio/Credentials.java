package io.github.ppzxc.strand.nio;

import java.util.Objects;
import java.util.regex.Pattern;

public final class Credentials {
    private static final Pattern MECHANISM = Pattern.compile("[A-Z0-9_-]{1,20}");

    private final String mechanism;
    private final byte[] secret;

    private Credentials(String mechanism, byte[] secret) {
        this.mechanism = mechanism;
        this.secret = secret;
    }

    public static Credentials of(String mechanism, byte[] secret) {
        Objects.requireNonNull(mechanism, "mechanism");
        Objects.requireNonNull(secret, "secret");
        if (!MECHANISM.matcher(mechanism).matches()) {
            throw new IllegalArgumentException("AUTH 메커니즘이 문법(대문자·숫자·하이픈·밑줄 1~20자)을 어긴다");
        }
        return new Credentials(mechanism, secret.clone());
    }

    public String mechanism() {
        return mechanism;
    }

    public byte[] secret() {
        return secret.clone();
    }

    @Override
    public String toString() {
        return "Credentials[mechanism=" + mechanism + ", secret=<redacted>]";
    }
}
