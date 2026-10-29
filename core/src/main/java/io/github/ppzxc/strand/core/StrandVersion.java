package io.github.ppzxc.strand.core;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.jspecify.annotations.Nullable;

/**
 * 실행 중인 core의 버전. 공개 전까지 바꾸거나 지울 수 있는 임시 API다.
 */
public final class StrandVersion {

    private StrandVersion() {}

    /**
     * core jar 매니페스트의 {@code Implementation-Version}을 반환한다.
     *
     * <p>{@code Package.getImplementationVersion()}은 모듈 경로에서 {@code null}이라, 클래스를 담은 jar의
     * 매니페스트를 직접 읽는다. 클래스패스와 모듈 경로에서 같은 값을 준다.
     *
     * @return 버전 문자열. jar가 아니거나 매니페스트에 값이 없으면 {@code null}
     */
    public static @Nullable String get() {
        return versionOf(StrandVersion.class.getProtectionDomain().getCodeSource());
    }

    /** 테스트가 임의의 코드 위치로 부를 수 있게 분리한 구현. */
    static @Nullable String versionOf(@Nullable CodeSource source) {
        if (source == null) {
            return null;
        }
        try {
            File file = new File(source.getLocation().toURI());
            if (!file.isFile()) {
                return null;
            }
            try (JarFile jar = new JarFile(file)) {
                Manifest manifest = jar.getManifest();
                return manifest == null
                        ? null
                        : manifest.getMainAttributes().getValue(Attributes.Name.IMPLEMENTATION_VERSION);
            }
        } catch (IOException | URISyntaxException | IllegalArgumentException e) {
            return null;
        }
    }
}
