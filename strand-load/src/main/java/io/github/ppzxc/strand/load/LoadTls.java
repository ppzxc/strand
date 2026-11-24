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

/**
 * 하네스의 TLS 설정. 양쪽 전송에 같은 정책을 강제해 TLS 설정 차이가 전송 차이로 보이지 않게 한다: TLS 1.3, 같은 암호 스위트, JDK 기본 provider. 정책은 자식 JVM 시스템 속성으로 강제하므로 두
 * 전송 모두 같은 규칙을 따른다. 키 자료는 실행마다 {@code keytool}로 만들어 임시 디렉터리에 두고 저장소에 두지 않는다.
 */
final class LoadTls {
    static final String PROTOCOL = "TLSv1.3";
    static final String CIPHER_SUITE = "TLS_AES_128_GCM_SHA256";
    static final String ALPN = "strand";
    private static final char[] PASSWORD = "changeit".toCharArray();

    private LoadTls() {}

    /** 자식 JVM에 붙이는 옵션: 프로토콜과 암호 스위트를 TLS 1.3 한 가지로 고정한다. */
    static List<String> jvmOptions() {
        return Arrays.asList(
                "-Djdk.tls.client.protocols=" + PROTOCOL,
                "-Djdk.tls.server.protocols=" + PROTOCOL,
                "-Djdk.tls.client.cipherSuites=" + CIPHER_SUITE,
                "-Djdk.tls.server.cipherSuites=" + CIPHER_SUITE);
    }

    /** localhost·127.0.0.1용 자체 서명 인증서가 든 PKCS12 키 저장소를 만든다. */
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
