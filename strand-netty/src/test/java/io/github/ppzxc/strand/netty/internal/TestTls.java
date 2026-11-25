package io.github.ppzxc.strand.netty.internal;

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

public final class TestTls {
    private static final char[] PASSWORD = "changeit".toCharArray();
    private static SSLContext server;
    private static SSLContext client;
    private static SSLContext stranger;

    private TestTls() {}

    public static synchronized SSLContext server() throws Exception {
        init();
        return server;
    }

    public static synchronized SSLContext client() throws Exception {
        init();
        return client;
    }

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
