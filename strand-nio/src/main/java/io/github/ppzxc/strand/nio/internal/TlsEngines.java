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

public final class TlsEngines {

    public static final String ALPN_IDENTIFIER = "strand";

    private static final List<String> PREFERRED_PROTOCOLS = Arrays.asList("TLSv1.3", "TLSv1.2");
    private static final Alpn SYSTEM = Alpn.detect();

    private TlsEngines() {}

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

    public static boolean alpnSupported() {
        return SYSTEM.supported();
    }

    public static SSLEngine client(SSLContext context, String host, int port) {
        return client(context, host, port, SYSTEM);
    }

    static SSLEngine client(SSLContext context, String host, int port, Alpn alpn) {
        SSLEngine engine = context.createSSLEngine(host, port);
        engine.setUseClientMode(true);
        configure(engine, true, alpn);
        return engine;
    }

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
