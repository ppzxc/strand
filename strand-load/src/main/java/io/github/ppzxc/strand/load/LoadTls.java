package io.github.ppzxc.strand.load;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

final class LoadTls {
    static final String PROTOCOL = "TLSv1.3";
    static final String CIPHER_SUITE = "TLS_AES_128_GCM_SHA256";
    static final String ALPN = "strand";
    private static final char[] PASSWORD = "changeit".toCharArray();

    private LoadTls() {}

    static List<String> jvmOptions() {
        return Arrays.asList(
                "-Djdk.tls.client.protocols=" + PROTOCOL,
                "-Djdk.tls.server.protocols=" + PROTOCOL,
                "-Djdk.tls.client.cipherSuites=" + CIPHER_SUITE,
                "-Djdk.tls.server.cipherSuites=" + CIPHER_SUITE);
    }

    static Path createKeystore(Path dir) throws IOException, InterruptedException {
        Path store = dir.resolve("load-ks.p12");
        String keytool =
                Paths.get(System.getProperty("java.home"), "bin", "keytool").toString();
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
            process.destroyForcibly();
            throw new IOException("keytool로 키 저장소를 만들지 못했다");
        }
        return store;
    }

    static void deleteTree(Path dir) throws IOException {
        try (var files = Files.walk(dir)) {
            files.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> p.toFile().delete());
        }
    }

    static SSLContext serverContext(Path store) throws Exception {
        KeyStore keyStore = load(store);
        KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(keyStore, PASSWORD);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys.getKeyManagers(), null, null);
        return context;
    }

    static SSLContext clientContext(Path store) throws Exception {
        TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(load(store));
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, trust.getTrustManagers(), null);
        return context;
    }

    private static KeyStore load(Path store) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(store)) {
            keyStore.load(in, PASSWORD);
        }
        return keyStore;
    }
}
