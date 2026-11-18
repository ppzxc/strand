package io.github.ppzxc.strand.nio.internal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * 테스트용 TLS 컨텍스트. 처음 쓸 때 JDK의 {@code keytool}로 localhost·127.0.0.1용 자체 서명 인증서를 만들어 서버는 그 키를, 클라이언트는 그
 * 인증서를 신뢰한다. 저장소에 키 자료를 두지 않는다.
 */
public final class TestTls {
    private static final char[] PASSWORD = "changeit".toCharArray();
    private static SSLContext server;
    private static SSLContext client;
    private static SSLContext stranger;

    private TestTls() {}

    /**
     * 서버 쪽 컨텍스트(키를 가진다).
     *
     * @return 서버 컨텍스트
     * @throws Exception 키 저장소를 만들거나 읽을 수 없을 때
     */
    public static synchronized SSLContext server() throws Exception {
        init();
        return server;
    }

    /**
     * 클라이언트 쪽 컨텍스트(서버 인증서를 신뢰한다).
     *
     * @return 클라이언트 컨텍스트
     * @throws Exception 키 저장소를 만들거나 읽을 수 없을 때
     */
    public static synchronized SSLContext client() throws Exception {
        init();
        return client;
    }

    /**
     * 서버 인증서를 신뢰하지 않는 클라이언트 컨텍스트(JDK 기본 신뢰 저장소).
     *
     * @return 서버를 신뢰하지 않는 컨텍스트
     * @throws Exception 컨텍스트를 만들 수 없을 때
     */
    public static synchronized SSLContext stranger() throws Exception {
        init();
        return stranger;
    }

    private static void init() throws Exception {
        if (server != null) {
            return;
        }
        Path dir = Files.createTempDirectory("strand-tls");
        Path store = dir.resolve("ks.p12");
        String keytool = java.nio.file.Paths.get(System.getProperty("java.home"), "bin", "keytool")
                .toString();
        Process process = new ProcessBuilder(Arrays.asList(
                        keytool,
                        "-genkeypair",
                        "-alias",
                        "strand",
                        "-keyalg",
                        "RSA",
                        "-keysize",
                        "2048",
                        "-validity",
                        "2",
                        "-dname",
                        "CN=localhost",
                        "-ext",
                        "san=dns:localhost,ip:127.0.0.1",
                        "-storetype",
                        "PKCS12",
                        "-keystore",
                        store.toString(),
                        "-storepass",
                        new String(PASSWORD),
                        "-keypass",
                        new String(PASSWORD)))
                .redirectErrorStream(true)
                .start();
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
            throw new IOException("keytool 실패");
        }
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(store)) {
            keyStore.load(in, PASSWORD);
        }
        KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(keyStore, PASSWORD);
        TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(keyStore);
        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(keys.getKeyManagers(), null, null);
        SSLContext clientContext = SSLContext.getInstance("TLS");
        clientContext.init(null, trust.getTrustManagers(), null);
        SSLContext strangerContext = SSLContext.getInstance("TLS");
        strangerContext.init(null, null, null);
        server = serverContext;
        client = clientContext;
        stranger = strangerContext;
    }
}
