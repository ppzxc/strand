package io.github.ppzxc.strand.core;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.function.Supplier;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.jspecify.annotations.Nullable;

public final class StrandVersion {

    private StrandVersion() {}

    public static @Nullable String get() {
        CodeSource source =
                codeSourceOf(() -> StrandVersion.class.getProtectionDomain().getCodeSource());
        return resolve(source, StrandVersion.class.getPackage().getImplementationVersion());
    }

    static @Nullable CodeSource codeSourceOf(Supplier<@Nullable CodeSource> supplier) {
        try {
            return supplier.get();
        } catch (SecurityException e) {
            return null;
        }
    }

    static @Nullable String resolve(@Nullable CodeSource source, @Nullable String packageVersion) {
        String fromManifest = versionOf(source);
        return fromManifest != null ? fromManifest : packageVersion;
    }

    static @Nullable String versionOf(@Nullable CodeSource source) {
        if (source == null || source.getLocation() == null) {
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
        } catch (IOException | URISyntaxException | IllegalArgumentException | SecurityException e) {
            return null;
        }
    }
}
