package io.github.ppzxc.strand.netty;

import java.util.Objects;
import java.util.regex.Pattern;

/** 연결 인증(규격 3.3.1절)에 쓰는 메커니즘 이름과 자격 증명. 자격 증명 원문은 {@link #toString}과 진단에 나타나지 않는다. */
public final class Credentials {
    private static final Pattern MECHANISM = Pattern.compile("[A-Z0-9_-]{1,20}");

    private final String mechanism;
    private final byte[] secret;

    private Credentials(String mechanism, byte[] secret) {
        this.mechanism = mechanism;
        this.secret = secret;
    }

    /**
     * 자격 증명을 만든다.
     *
     * @param mechanism 메커니즘 이름(대문자·숫자·하이픈·밑줄 1~20자)
     * @param secret 메커니즘이 정한 불투명 자격 증명. 복사해 보관한다
     * @return 자격 증명
     * @throws IllegalArgumentException 메커니즘 이름이 문법을 어길 때
     */
    public static Credentials of(String mechanism, byte[] secret) {
        Objects.requireNonNull(mechanism, "mechanism");
        Objects.requireNonNull(secret, "secret");
        if (!MECHANISM.matcher(mechanism).matches()) {
            throw new IllegalArgumentException("AUTH 메커니즘이 문법(대문자·숫자·하이픈·밑줄 1~20자)을 어긴다");
        }
        return new Credentials(mechanism, secret.clone());
    }

    /**
     * 메커니즘 이름.
     *
     * @return 메커니즘 이름
     */
    public String mechanism() {
        return mechanism;
    }

    /**
     * 자격 증명 원문. 부를 때마다 복사본을 돌려준다.
     *
     * @return 자격 증명의 복사본
     */
    public byte[] secret() {
        return secret.clone();
    }

    @Override
    public String toString() {
        return "Credentials[mechanism=" + mechanism + ", secret=<redacted>]";
    }
}
