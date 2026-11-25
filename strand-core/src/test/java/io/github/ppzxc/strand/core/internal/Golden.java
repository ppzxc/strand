package io.github.ppzxc.strand.core.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

final class Golden {

    private static final Path RESOURCE_DIR = Paths.get("src/test/resources/io/github/ppzxc/strand/core/internal");

    private Golden() {}

    static String sha256(String text) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static List<String> lines(String resource, List<String> actual) {
        if ("1".equals(System.getenv("STRAND_GOLDEN_UPDATE"))) {
            if (System.getenv("CI") != null) {

                throw new IllegalStateException("CI에서는 STRAND_GOLDEN_UPDATE를 쓸 수 없다");
            }
            try {
                Files.write(
                        RESOURCE_DIR.resolve(resource),
                        (String.join("\n", actual) + "\n").getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            return actual;
        }
        try (InputStream in = Golden.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("골든 파일이 없다: " + resource);
            }
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            for (int read = in.read(buffer); read >= 0; read = in.read(buffer)) {
                bytes.write(buffer, 0, read);
            }
            return Arrays.asList(new String(bytes.toByteArray(), StandardCharsets.UTF_8).split("\n"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
