package io.github.ppzxc.strand.nio.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.jspecify.annotations.Nullable;

/**
 * strand over TCP의 TLS 엔진을 규격 3.7절 조건으로 만든다. TLS 1.3을 우선하되 1.2까지만 켜고(BCP 195), 클라이언트는 서버 인증서의 호스트 이름을
 * 검증하며, ALPN 식별자 {@value #ALPN_IDENTIFIER}를 제시하거나 요구한다. 0-RTT는 JDK가 켜지 않는다. 사후 인증과 재협상은 통로가 거절한다.
 *
 * <p>ALPN은 Java 9에서 들어오고 8u252에 이식된 API라 {@code --release 8}로 컴파일하는 이 모듈은 리플렉션으로 쓴다. 런타임이 지원하지 않으면 TLS를
 * 만들 수 없다.
 */
public final class TlsEngines {
    /** strand over TCP의 ALPN 식별자(규격 3.7절). */
    public static final String ALPN_IDENTIFIER = "strand";

    private static final List<String> PREFERRED_PROTOCOLS = Arrays.asList("TLSv1.3", "TLSv1.2");
    private static final Alpn SYSTEM = Alpn.detect();

    private TlsEngines() {}

    /** 런타임의 ALPN API. 리플렉션으로 찾으며, 찾지 못하면 지원하지 않는 것이다. 테스트가 지원하지 않는 런타임을 흉내 낼 수 있게 분리했다. */
    static final class Alpn {
        private final @Nullable Method setter;
        private final @Nullable Method getter;

        Alpn(@Nullable Method setter, @Nullable Method getter) {
            this.setter = setter;
            this.getter = getter;
        }

        static Alpn detect() {
            return new Alpn(
                    method(SSLParameters.class, "setApplicationProtocols", String[].class),
                    method(SSLEngine.class, "getApplicationProtocol"));
        }

        static Alpn unsupported() {
            return new Alpn(null, null);
        }

        private static @Nullable Method method(Class<?> type, String name, Class<?>... parameters) {
            try {
                return type.getMethod(name, parameters);
            } catch (NoSuchMethodException e) {
                return null;
            }
        }

        boolean supported() {
            return setter != null && getter != null;
        }
    }

    /**
     * 이 런타임이 ALPN을 지원하는가.
     *
     * @return 지원하면 true
     */
    public static boolean alpnSupported() {
        return SYSTEM.supported();
    }

    /**
     * 클라이언트 엔진. 서버 호스트 이름을 검증한다.
     *
     * @param context TLS 컨텍스트
     * @param host 서버 인증서가 가리켜야 하는 호스트 이름 또는 주소
     * @param port 서버 포트
     * @return 클라이언트 엔진
     * @throws IllegalStateException 런타임이 ALPN을 지원하지 않을 때
     */
    public static SSLEngine client(SSLContext context, String host, int port) {
        return client(context, host, port, SYSTEM);
    }

    static SSLEngine client(SSLContext context, String host, int port, Alpn alpn) {
        SSLEngine engine = context.createSSLEngine(host, port);
        engine.setUseClientMode(true);
        configure(engine, true, alpn);
        return engine;
    }

    /**
     * 서버 엔진. 클라이언트 인증서는 요구하지 않는다(규격 3.7절: 선택).
     *
     * @param context TLS 컨텍스트
     * @return 서버 엔진
     * @throws IllegalStateException 런타임이 ALPN을 지원하지 않을 때
     */
    public static SSLEngine server(SSLContext context) {
        return server(context, SYSTEM);
    }

    static SSLEngine server(SSLContext context, Alpn alpn) {
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        configure(engine, false, alpn);
        return engine;
    }

    private static void configure(SSLEngine engine, boolean verifyHostName, Alpn alpn) {
        if (!alpn.supported()) {
            throw new IllegalStateException("이 런타임은 ALPN을 지원하지 않아 strand over TCP의 TLS를 쓸 수 없다");
        }
        Method setter = Objects.requireNonNull(alpn.setter);
        List<String> supported = Arrays.asList(engine.getSupportedProtocols());
        List<String> enabled = new ArrayList<>(PREFERRED_PROTOCOLS);
        enabled.retainAll(supported);
        if (enabled.isEmpty()) {
            throw new IllegalStateException("TLS 1.2 이상을 지원하는 런타임이 필요하다");
        }
        engine.setEnabledProtocols(enabled.toArray(new String[0]));
        SSLParameters parameters = engine.getSSLParameters();
        if (verifyHostName) {
            parameters.setEndpointIdentificationAlgorithm("HTTPS");
        }
        try {
            setter.invoke(parameters, (Object) new String[] {ALPN_IDENTIFIER});
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("ALPN 식별자를 설정하지 못했다", e);
        }
        engine.setSSLParameters(parameters);
    }

    /**
     * 핸드셰이크가 끝난 엔진이 협상한 ALPN 식별자. 협상하지 않았거나 런타임이 지원하지 않으면 null이다.
     *
     * @param engine 엔진
     * @return 협상한 식별자 또는 null
     */
    public static @Nullable String negotiatedProtocol(SSLEngine engine) {
        return negotiatedProtocol(engine, SYSTEM);
    }

    static @Nullable String negotiatedProtocol(SSLEngine engine, Alpn alpn) {
        Method getter = alpn.getter;
        if (getter == null) {
            return null;
        }
        try {
            String protocol = (String) getter.invoke(engine);
            return protocol == null || protocol.isEmpty() ? null : protocol;
        } catch (IllegalAccessException | InvocationTargetException e) {
            return null;
        }
    }
}
