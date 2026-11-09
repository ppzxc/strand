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

/**
 * 골든 마스터 파일 도우미(#200). 골든은 구현을 바꾸기 전 코드가 낸 값이라, 바꾼 뒤에도 같은 값이 나오면 동작이 같다는 근거다. 다시 만들 때는
 * 환경 변수 {@code STRAND_GOLDEN_UPDATE=1}로 테스트를 돌린다. 규격이 보내는 바이트를 일부러 바꾼 변경에서만 쓴다.
 */
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

    /** 골든 파일의 줄. 환경 변수로 갱신을 요청했으면 {@code actual}을 먼저 파일에 쓴다. */
    static List<String> lines(String resource, List<String> actual) {
        if ("1".equals(System.getenv("STRAND_GOLDEN_UPDATE"))) {
            try {
                Files.write(
                        RESOURCE_DIR.resolve(resource),
                        (String.join("\n", actual) + "\n").getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            // 갱신한 값을 그대로 돌려줘 이번 실행을 통과시킨다. 다음 실행부터 파일을 읽는다.
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
