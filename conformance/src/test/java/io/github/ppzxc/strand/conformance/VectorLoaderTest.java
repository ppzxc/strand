package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class VectorLoaderTest {

    private static final String VALID = "{\"id\":\"a\",\"description\":\"d\",\"result\":\"valid\",\"bytes\":\"00ff\"}";

    private static void assertRejected(String json, String... messageParts) {
        assertThatThrownBy(() -> VectorLoader.parse("x.json", json))
                .isInstanceOf(InvalidVectorException.class)
                .hasMessageContaining("x.json")
                .hasMessageContainingAll(messageParts);
    }

    @Test
    void loadsWellFormedVectors() {
        String json = "[{\"id\":\"a\",\"description\":\"첫째\",\"result\":\"valid\",\"bytes\":\"00 ff\\n10\"},"
                + "{\"id\":\"b\",\"description\":\"둘째\",\"result\":\"invalid\",\"bytes\":\"\"}]";

        List<ConformanceVector> vectors = VectorLoader.parse("x.json", json);

        assertThat(vectors).hasSize(2);
        assertThat(vectors.get(0).id()).isEqualTo("a");
        assertThat(vectors.get(0).description()).isEqualTo("첫째");
        assertThat(vectors.get(0).result()).isEqualTo(ConformanceVector.Result.VALID);
        assertThat(vectors.get(0).bytes()).containsExactly((byte) 0x00, (byte) 0xff, (byte) 0x10);
        assertThat(vectors.get(1).result()).isEqualTo(ConformanceVector.Result.INVALID);
        assertThat(vectors.get(1).bytes()).isEmpty();
    }

    @Test
    void rejectsUnknownField() {
        assertRejected(
                "[{\"id\":\"a\",\"description\":\"d\",\"result\":\"valid\",\"bytes\":\"00\",\"extra\":\"x\"}]",
                "extra");
    }

    @Test
    void rejectsMissingId() {
        assertRejected("[{\"description\":\"d\",\"result\":\"valid\",\"bytes\":\"00\"}]", "필수 필드 누락", "id");
    }

    @Test
    void rejectsMissingDescription() {
        assertRejected("[{\"id\":\"a\",\"result\":\"valid\",\"bytes\":\"00\"}]", "필수 필드 누락", "description", "a");
    }

    @Test
    void rejectsMissingResult() {
        assertRejected("[{\"id\":\"a\",\"description\":\"d\",\"bytes\":\"00\"}]", "필수 필드 누락", "result", "a");
    }

    @Test
    void rejectsMissingBytes() {
        assertRejected("[{\"id\":\"a\",\"description\":\"d\",\"result\":\"valid\"}]", "필수 필드 누락", "bytes", "a");
    }

    @Test
    void rejectsResultOutsideAllowedValues() {
        assertRejected(
                "[{\"id\":\"a\",\"description\":\"d\",\"result\":\"maybe\",\"bytes\":\"00\"}]", "result", "maybe", "a");
    }

    @Test
    void rejectsOddLengthHex() {
        assertRejected("[{\"id\":\"a\",\"description\":\"d\",\"result\":\"valid\",\"bytes\":\"00f\"}]", "홀수", "a");
    }

    @Test
    void rejectsNonHexCharacter() {
        assertRejected("[{\"id\":\"a\",\"description\":\"d\",\"result\":\"valid\",\"bytes\":\"00zz\"}]", "hex", "a");
    }

    @Test
    void rejectsUppercaseHex() {
        assertRejected("[{\"id\":\"a\",\"description\":\"d\",\"result\":\"valid\",\"bytes\":\"00FF\"}]", "대문자", "a");
    }

    @Test
    void rejectsDuplicateIdInFile() {
        assertRejected("[" + VALID + "," + VALID.replace("\"a\"", "\"b\"") + "," + VALID + "]", "id 중복", "a");
    }

    @Test
    void rejectsDuplicateKeyInObject() {
        // Gson 트리 API는 같은 키를 조용히 덮어쓰므로 로더가 직접 걸러야 한다.
        assertRejected(
                "[{\"id\":\"a\",\"description\":\"d\",\"result\":\"valid\",\"result\":\"invalid\",\"bytes\":\"00\"}]",
                "중복 키",
                "result");
    }

    @Test
    void rejectsEmptyId() {
        assertRejected("[{\"id\":\"\",\"description\":\"d\",\"result\":\"valid\",\"bytes\":\"00\"}]", "id", "비어");
    }

    @Test
    void rejectsNonStringId() {
        assertRejected("[{\"id\":1,\"description\":\"d\",\"result\":\"valid\",\"bytes\":\"00\"}]", "id", "문자열");
    }

    @Test
    void rejectsNonStringResult() {
        assertRejected("[{\"id\":\"a\",\"description\":\"d\",\"result\":1,\"bytes\":\"00\"}]", "result", "문자열", "a");
    }

    @Test
    void rejectsTopLevelThatIsNotAnArray() {
        assertRejected(VALID, "배열");
    }

    @Test
    void rejectsArrayElementThatIsNotAnObject() {
        assertRejected("[\"a\"]", "[0]", "객체");
    }

    @Test
    void rejectsJsonSyntaxError() {
        assertRejected("[{\"id\":", "JSON");
    }

    @Test
    void rejectsTrailingContent() {
        assertRejected("[] []", "JSON");
    }

    @Test
    void rejectsEmptyFile() {
        assertRejected("  \n", "비어");
    }

    @Test
    void acceptsEmptyArray() {
        assertThat(VectorLoader.parse("x.json", "[]")).isEmpty();
    }
}
