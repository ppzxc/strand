package io.github.ppzxc.strand.netty.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.junit.jupiter.api.Test;

class TlsEnginesTest {

    @Test
    void thisRuntimeSupportsAlpn() {
        assertThat(TlsEngines.alpnSupported()).isTrue();
    }

    @Test
    void aRuntimeWithoutAlpnCannotCreateEnginesBecauseTheClientMustOfferTheIdentifier() throws Exception {
        SSLContext context = TestTls.client();

        assertThatThrownBy(() -> TlsEngines.client(context, "localhost", 1, TlsEngines.Alpn.unsupported()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ALPN");
        assertThatThrownBy(() -> TlsEngines.server(TestTls.server(), TlsEngines.Alpn.unsupported()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(TlsEngines.Alpn.unsupported().supported()).isFalse();
    }

    @Test
    void alpnIsSupportedOnlyWhenBothTheSetterAndTheGetterExist() throws Exception {
        java.lang.reflect.Method method = Object.class.getMethod("toString");

        assertThat(new TlsEngines.Alpn(method, method).supported()).isTrue();
        assertThat(new TlsEngines.Alpn(method, null).supported()).isFalse();
        assertThat(new TlsEngines.Alpn(null, method).supported()).isFalse();
    }

    @Test
    void anEngineThatNeverNegotiatedHasNoProtocol() throws Exception {
        SSLEngine engine = TlsEngines.client(TestTls.client(), "localhost", 1);

        assertThat(TlsEngines.negotiatedProtocol(engine)).isNull();
        assertThat(TlsEngines.negotiatedProtocol(engine, TlsEngines.Alpn.unsupported()))
                .isNull();
    }

    @Test
    void theNegotiatedProtocolOfAnEngineWithoutALegibleGetterIsNull() throws Exception {
        SSLEngine engine = TlsEngines.client(TestTls.client(), "localhost", 1);
        java.lang.reflect.Method notAGetter = Object.class.getMethod("toString");

        assertThat(TlsEngines.negotiatedProtocol(engine, new TlsEngines.Alpn(null, notAGetter)))
                .isNotNull();
        java.lang.reflect.Method failing = SSLEngine.class.getMethod("getHandshakeSession");
        assertThat(TlsEngines.negotiatedProtocol(engine, new TlsEngines.Alpn(null, failing)))
                .isNull();
    }

    @Test
    void anEngineWhoseSupportedProtocolsLackTls12IsRefused() throws Exception {
        SSLContext legacy = SSLContext.getInstance("TLS");
        legacy.init(null, null, null);
        SSLContext only11 = new SSLContext(
                new javax.net.ssl.SSLContextSpi() {
                    @Override
                    protected void engineInit(
                            javax.net.ssl.KeyManager[] km,
                            javax.net.ssl.TrustManager[] tm,
                            java.security.SecureRandom sr) {}

                    @Override
                    protected javax.net.ssl.SSLSocketFactory engineGetSocketFactory() {
                        return legacy.getSocketFactory();
                    }

                    @Override
                    protected javax.net.ssl.SSLServerSocketFactory engineGetServerSocketFactory() {
                        return legacy.getServerSocketFactory();
                    }

                    @Override
                    protected SSLEngine engineCreateSSLEngine() {
                        return restricted(legacy.createSSLEngine());
                    }

                    @Override
                    protected SSLEngine engineCreateSSLEngine(String host, int port) {
                        return restricted(legacy.createSSLEngine(host, port));
                    }

                    @Override
                    protected javax.net.ssl.SSLSessionContext engineGetServerSessionContext() {
                        return legacy.getServerSessionContext();
                    }

                    @Override
                    protected javax.net.ssl.SSLSessionContext engineGetClientSessionContext() {
                        return legacy.getClientSessionContext();
                    }
                },
                legacy.getProvider(),
                "TLS") {};

        assertThatThrownBy(() -> TlsEngines.server(only11))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TLS 1.2");
    }

    private static SSLEngine restricted(SSLEngine real) {
        return new javax.net.ssl.SSLEngine() {
            @Override
            public String[] getSupportedProtocols() {
                return new String[] {"TLSv1.1", "TLSv1"};
            }

            @Override
            public javax.net.ssl.SSLEngineResult wrap(java.nio.ByteBuffer[] a, int o, int l, java.nio.ByteBuffer d)
                    throws javax.net.ssl.SSLException {
                return real.wrap(a, o, l, d);
            }

            @Override
            public javax.net.ssl.SSLEngineResult unwrap(java.nio.ByteBuffer s, java.nio.ByteBuffer[] d, int o, int l)
                    throws javax.net.ssl.SSLException {
                return real.unwrap(s, d, o, l);
            }

            @Override
            public Runnable getDelegatedTask() {
                return real.getDelegatedTask();
            }

            @Override
            public void closeInbound() throws javax.net.ssl.SSLException {
                real.closeInbound();
            }

            @Override
            public boolean isInboundDone() {
                return real.isInboundDone();
            }

            @Override
            public void closeOutbound() {
                real.closeOutbound();
            }

            @Override
            public boolean isOutboundDone() {
                return real.isOutboundDone();
            }

            @Override
            public String[] getSupportedCipherSuites() {
                return real.getSupportedCipherSuites();
            }

            @Override
            public String[] getEnabledCipherSuites() {
                return real.getEnabledCipherSuites();
            }

            @Override
            public void setEnabledCipherSuites(String[] s) {
                real.setEnabledCipherSuites(s);
            }

            @Override
            public String[] getEnabledProtocols() {
                return real.getEnabledProtocols();
            }

            @Override
            public void setEnabledProtocols(String[] p) {
                real.setEnabledProtocols(p);
            }

            @Override
            public javax.net.ssl.SSLSession getSession() {
                return real.getSession();
            }

            @Override
            public void beginHandshake() throws javax.net.ssl.SSLException {
                real.beginHandshake();
            }

            @Override
            public javax.net.ssl.SSLEngineResult.HandshakeStatus getHandshakeStatus() {
                return real.getHandshakeStatus();
            }

            @Override
            public void setUseClientMode(boolean mode) {
                real.setUseClientMode(mode);
            }

            @Override
            public boolean getUseClientMode() {
                return real.getUseClientMode();
            }

            @Override
            public void setNeedClientAuth(boolean need) {
                real.setNeedClientAuth(need);
            }

            @Override
            public boolean getNeedClientAuth() {
                return real.getNeedClientAuth();
            }

            @Override
            public void setWantClientAuth(boolean want) {
                real.setWantClientAuth(want);
            }

            @Override
            public boolean getWantClientAuth() {
                return real.getWantClientAuth();
            }

            @Override
            public void setEnableSessionCreation(boolean flag) {
                real.setEnableSessionCreation(flag);
            }

            @Override
            public boolean getEnableSessionCreation() {
                return real.getEnableSessionCreation();
            }
        };
    }
}
