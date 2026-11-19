package io.github.ppzxc.strand.netty;

import java.util.Objects;
import javax.net.ssl.SSLContext;
import org.jspecify.annotations.Nullable;

/**
 * 연결 채널의 보안(규격 3.7절). Strand는 기밀성·무결성·서버 인증을 직접 제공하지 않으므로 이 셋을 모두 제공하는 채널 위에서만 쓴다. 보안 조건이
 * 충족된 뒤에야 HELLO를 시작한다.
 */
public final class NettySecurity {
    private final @Nullable SSLContext tlsContext;

    private NettySecurity(@Nullable SSLContext tlsContext) {
        this.tlsContext = tlsContext;
    }

    /**
     * TLS로 채널을 보호한다. TLS 1.3을 우선하고 1.2까지만 켠다. 클라이언트는 서버 인증서의 호스트 이름을 검증하고, ALPN 식별자 {@code strand}를
     * 제시(클라이언트)하거나 요구(서버)한다. 인증서·신뢰 저장소는 컨텍스트가 정한다. TLS 협상에 실패하면 평문으로 바꾸지 않는다.
     *
     * @param context 앱이 구성한 TLS 컨텍스트. 서버는 키를, 클라이언트는 신뢰 저장소를 가져야 한다
     * @return 보안 설정
     */
    public static NettySecurity tls(SSLContext context) {
        return new NettySecurity(Objects.requireNonNull(context, "context"));
    }

    /**
     * TCP 밖의 수단(IPsec, 보호 터널 등)이 기밀성·무결성·서버 인증을 이미 제공한다는 것을 앱이 보증한다. 이 모듈은 그 성질을 검증하지 않는다. 보증할
     * 수 없는 곳에서는 {@link #tls}를 쓴다.
     *
     * @return 외부에서 보호되는 채널 설정
     */
    public static NettySecurity externallySecured() {
        return new NettySecurity(null);
    }

    @Nullable
    SSLContext tlsContext() {
        return tlsContext;
    }
}
