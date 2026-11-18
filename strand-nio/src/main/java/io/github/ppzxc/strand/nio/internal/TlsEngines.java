package io.github.ppzxc.strand.nio.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
final class TlsEngines {
    /** strand over TCP의 ALPN 식별자(규격 3.7절). */
    static final String ALPN_IDENTIFIER = "strand";

    private static final List<String> PREFERRED_PROTOCOLS = Arrays.asList("TLSv1.3", "TLSv1.2");
    private static final @Nullable Method SET_APPLICATION_PROTOCOLS =
            method(SSLParameters.class, "setApplicationProtocols", String[].class);
    private static final @Nullable Method GET_APPLICATION_PROTOCOL = method(SSLEngine.class, "getApplicationProtocol");

    private TlsEngines() {}

    private static @Nullable Method method(Class<?> type, String name, Class<?>... parameters) {
        try {
            return type.getMethod(name, parameters);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * 이 런타임이 ALPN을 지원하는가.
     *
     * @return 지원하면 true
     */
    static boolean alpnSupported() {
        return SET_APPLICATION_PROTOCOLS != null && GET_APPLICATION_PROTOCOL != null;
    }

    /**
     * 클라이언트 엔진. 서버 호스트 이름을 검증한다.
     *
     * @param host 서버 인증서가 가리켜야 하는 호스트 이름 또는 주소
     * @param port 서버 포트
     * @throws IllegalStateException 런타임이 ALPN을 지원하지 않을 때
     */
    static SSLEngine client(SSLContext context, String host, int port) {
        SSLEngine engine = context.createSSLEngine(host, port);
        engine.setUseClientMode(true);
        configure(engine, true);
        return engine;
    }

    /**
     * 서버 엔진. 클라이언트 인증서는 요구하지 않는다(규격 3.7절: 선택).
     *
     * @throws IllegalStateException 런타임이 ALPN을 지원하지 않을 때
     */
    static SSLEngine server(SSLContext context) {
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        configure(engine, false);
        return engine;
    }

    private static void configure(SSLEngine engine, boolean verifyHostName) {
        Method setter = SET_APPLICATION_PROTOCOLS;
        if (setter == null || GET_APPLICATION_PROTOCOL == null) {
            throw new IllegalStateException("이 런타임은 ALPN을 지원하지 않아 strand over TCP의 TLS를 쓸 수 없다");
        }
        List<String> supported = Arrays.asList(engine.getSupportedProtocols());
        List<String> enabled = new ArrayList<>();
        for (String protocol : PREFERRED_PROTOCOLS) {
            if (supported.contains(protocol)) {
                enabled.add(protocol);
            }
        }
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

    /** 핸드셰이크가 끝난 엔진이 협상한 ALPN 식별자. 협상하지 않았으면 null이다. */
    static @Nullable String negotiatedProtocol(SSLEngine engine) {
        Method getter = GET_APPLICATION_PROTOCOL;
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
