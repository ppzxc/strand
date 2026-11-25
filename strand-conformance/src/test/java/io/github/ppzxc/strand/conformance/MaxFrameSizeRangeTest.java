package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class MaxFrameSizeRangeTest {

    private static final Path SPEC = RequirementIndex.specificationFile();
    private static final Path FORMAT =
            RequirementIndex.repositoryRoot().resolve("docs/specification/STRAND_CONFORMANCE_VECTORS.md");

    private static final long KIB_16 = 16L * 1024;
    private static final long MIB_1 = 1024L * 1024;

    private static final List<String> OLD_VALUES =
            Arrays.asList("4 MiB", "16 MiB", "4,194,304", "16,777,216", "16,777,217");

    private static String read(Path file) throws IOException {
        return String.join("\n", Files.readAllLines(file, StandardCharsets.UTF_8));
    }

    private static String helloValue(String hex) {

        return hex.substring(hex.length() - 8);
    }

    private static long helloValueAt(String key, int step) {
        JsonObject vector = RawVectors.byKey().get(key);
        assertThat(vector).as(key).isNotNull();
        String hex = vector.getAsJsonArray("steps")
                .get(step)
                .getAsJsonObject()
                .get("hex")
                .getAsString();
        return Long.parseLong(helloValue(hex), 16);
    }

    @Test
    void specificationDefinesTheDefaultAndTheRangeInTheParameterTable() throws IOException {
        String row = null;
        for (String line : Files.readAllLines(SPEC, StandardCharsets.UTF_8)) {
            if (line.startsWith("| `0x0001` | `max_frame_size`")) {
                row = line;
            }
        }
        assertThat(row).isNotNull().contains("65,536 (64 KiB)").contains("16 KiB\\~1 MiB");
    }

    @Test
    void noOldDefaultOrLimitRemainsInSpecificationOrFormatOrVectors() throws IOException {
        List<String> texts = new ArrayList<>();
        texts.add(read(SPEC));
        texts.add(read(FORMAT));
        for (Path file : RawVectors.files()) {
            texts.add(read(file));
        }
        for (String text : texts) {
            assertThat(text).doesNotContain(OLD_VALUES);
        }
    }

    @Test
    void clientHelloBoundaryVectorsUseTheNewRange() {
        String p = "hello-establishment/negotiation-";
        assertThat(helloValueAt(p + "accepts-max-frame-size-at-range-minimum", 1))
                .isEqualTo(KIB_16);
        assertThat(helloValueAt(p + "accepts-max-frame-size-at-range-maximum", 1))
                .isEqualTo(MIB_1);
        assertThat(helloValueAt(p + "fails-when-client-max-frame-size-below-range", 1))
                .isEqualTo(KIB_16 - 1);
        assertThat(helloValueAt(p + "fails-when-client-max-frame-size-above-range", 1))
                .isEqualTo(MIB_1 + 1);
    }

    @Test
    void serverHelloBoundaryVectorsUseTheNewRange() {
        String p = "hello-establishment/client-";
        assertThat(helloValueAt(p + "accepts-server-max-frame-size-at-range-minimum", 0))
                .isEqualTo(KIB_16);
        assertThat(helloValueAt(p + "accepts-server-max-frame-size-at-range-maximum", 0))
                .isEqualTo(MIB_1);
        assertThat(helloValueAt(p + "rejects-server-max-frame-size-below-range", 0))
                .isEqualTo(KIB_16 - 1);
        assertThat(helloValueAt(p + "rejects-server-max-frame-size-above-range", 0))
                .isEqualTo(MIB_1 + 1);
    }
}
