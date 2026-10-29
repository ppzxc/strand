package io.github.ppzxc.strand.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.cert.Certificate;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StrandVersionTest {

    private static final String CLASS_ENTRY = "io/github/ppzxc/strand/core/StrandVersion.class";

    @TempDir
    Path tempDir;

    @Test
    void returnsImplementationVersionFromJarManifest() throws Exception {
        Path jar = jarWithManifest("1.2.3");

        assertThat(versionLoadedFrom(jar)).isEqualTo("1.2.3");
    }

    @Test
    void returnsNullWhenManifestHasNoImplementationVersion() throws Exception {
        Path jar = jarWithManifest(null);

        assertThat(versionLoadedFrom(jar)).isNull();
    }

    @Test
    void returnsNullWhenClassesAreNotPackagedInJar() {
        // 이 테스트는 클래스 디렉터리에서 실행되므로 매니페스트가 보이지 않는다.
        assertThat(StrandVersion.get()).isNull();
    }

    @Test
    void readsManifestOfTheJarGivenAsCodeSource() throws Exception {
        Path jar = jarWithManifest("4.5.6");

        assertThat(StrandVersion.versionOf(codeSource(jar.toUri().toURL()))).isEqualTo("4.5.6");
    }

    @Test
    void returnsNullWhenJarHasNoManifest() throws Exception {
        Path jar = Files.createTempFile(tempDir, "no-manifest", ".jar");
        try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(jar))) {
            jos.putNextEntry(new ZipEntry("x.txt"));
            jos.closeEntry();
        }

        assertThat(StrandVersion.versionOf(codeSource(jar.toUri().toURL()))).isNull();
    }

    @Test
    void returnsNullForUnreadableOrUnusableCodeSource() throws Exception {
        Path notAJar = Files.write(Files.createTempFile(tempDir, "not-a-jar", ".jar"), new byte[] {1, 2, 3});

        assertThat(StrandVersion.versionOf(null)).isNull();
        assertThat(StrandVersion.versionOf(codeSource(tempDir.toUri().toURL()))).isNull();
        assertThat(StrandVersion.versionOf(codeSource(notAJar.toUri().toURL()))).isNull();
        assertThat(StrandVersion.versionOf(codeSource(new URL("https://example.invalid/strand-core.jar"))))
                .isNull();
    }

    private static CodeSource codeSource(URL location) {
        return new CodeSource(location, (Certificate[]) null);
    }

    /**
     * 매니페스트가 보이는 경우는 jar로만 만들 수 있다. 같은 클래스를 임시 jar에 담아 부모 없는 클래스 로더로 읽는다.
     */
    private Path jarWithManifest(String implementationVersion) throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        if (implementationVersion != null) {
            manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, implementationVersion);
        }
        Path jar = Files.createTempFile(tempDir, "strand-version", ".jar");
        try (OutputStream out = Files.newOutputStream(jar);
                JarOutputStream jos = new JarOutputStream(out, manifest);
                InputStream classBytes = openClassBytes()) {
            jos.putNextEntry(new ZipEntry(CLASS_ENTRY));
            byte[] buffer = new byte[4096];
            int read;
            while ((read = classBytes.read(buffer)) != -1) {
                jos.write(buffer, 0, read);
            }
            jos.closeEntry();
        }
        return jar;
    }

    private static InputStream openClassBytes() {
        InputStream stream = StrandVersion.class.getClassLoader().getResourceAsStream(CLASS_ENTRY);
        if (stream == null) {
            throw new IllegalStateException("테스트 클래스패스에서 " + CLASS_ENTRY + "를 찾을 수 없다");
        }
        return stream;
    }

    private static String versionLoadedFrom(Path jar) throws Exception {
        URL[] urls = {jar.toUri().toURL()};
        try (URLClassLoader loader = new URLClassLoader(urls, null)) {
            Class<?> type = loader.loadClass("io.github.ppzxc.strand.core.StrandVersion");
            return (String) type.getMethod("get").invoke(null);
        }
    }
}
