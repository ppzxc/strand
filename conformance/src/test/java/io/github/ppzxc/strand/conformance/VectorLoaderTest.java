package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class VectorLoaderTest {

    private static String vector(String id) {
        return "{\"id\":\"" + id + "\",\"description\":\"d\",\"result\":\"valid\",\"bytes\":\"00ff\"}";
    }

    @Test
    void loadsWellFormedVectors() {
        String json = "[{\"id\":\"a\",\"description\":\"첫째\",\"result\":\"valid\",\"bytes\":\"00 ff\\n10\"},"
                + "{\"id\":\"b\",\"description\":\"둘째\",\"result\":\"invalid\",\"bytes\":\"\"}]";

        List<Vector> vectors = VectorLoader.parse("x.json", json);

        assertThat(vectors).hasSize(2);
        assertThat(vectors.get(0).id()).isEqualTo("a");
        assertThat(vectors.get(0).description()).isEqualTo("첫째");
        assertThat(vectors.get(0).result()).isEqualTo(Vector.Result.VALID);
        assertThat(vectors.get(0).bytes()).containsExactly((byte) 0x00, (byte) 0xff, (byte) 0x10);
        assertThat(vectors.get(1).result()).isEqualTo(Vector.Result.INVALID);
        assertThat(vectors.get(1).bytes()).isEmpty();
    }

    @Test
    void rejectsUnknownField() {
        String json = "[{\"id\":\"a\",\"description\":\"d\",\"result\":\"valid\",\"bytes\":\"00\",\"extra\":\"x\"}]";

        assertThatThrownBy(() -> VectorLoader.parse("x.json", json))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContaining("x.json")
                .hasMessageContaining("extra");
    }

    @Test
    void rejectsMissingRequiredField() {
        for (String field : new String[] {"id", "description", "result", "bytes"}) {
            String json = vector("a").replaceAll(",?\"" + field + "\":\"[^\"]*\"", "");
            json = json.replace("{,", "{");

            String broken = "[" + json + "]";
            assertThatThrownBy(() -> VectorLoader.parse("x.json", broken))
                    .as("누락 필드 %s", field)
                    .isInstanceOf(InvalidVectorException.class)
                    .hasMessageContaining(field);
        }
    }

    @Test
    void rejectsResultOutsideAllowedValues() {
        String json = vector("a").replace("\"valid\"", "\"maybe\"");

        assertThatThrownBy(() -> VectorLoader.parse("x.json", "[" + json + "]"))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContaining("result")
                .hasMessageContaining("maybe");
    }

    @Test
    void rejectsOddLengthHex() {
        String json = vector("a").replace("00ff", "00f");

        assertThatThrownBy(() -> VectorLoader.parse("x.json", "[" + json + "]"))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContaining("홀수");
    }

    @Test
    void rejectsNonHexCharacter() {
        String json = vector("a").replace("00ff", "00zz");

        assertThatThrownBy(() -> VectorLoader.parse("x.json", "[" + json + "]"))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContaining("hex");
    }

    @Test
    void rejectsUppercaseHex() {
        String json = vector("a").replace("00ff", "00FF");

        assertThatThrownBy(() -> VectorLoader.parse("x.json", "[" + json + "]"))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContaining("대문자");
    }

    @Test
    void rejectsDuplicateIdInFile() {
        String json = "[" + vector("a") + "," + vector("b") + "," + vector("a") + "]";

        assertThatThrownBy(() -> VectorLoader.parse("x.json", json))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContaining("중복")
                .hasMessageContaining("a");
    }
}
