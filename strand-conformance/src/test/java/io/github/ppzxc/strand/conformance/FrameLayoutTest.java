package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/** 규격 0.2.0의 20바이트 헤더와 64비트 {@code stream_id}가 벡터 hex에 반영됐는지 본다(#160). core를 실행하지 않는다. */
class FrameLayoutTest {

    private static final int HEADER_SIZE = 20;
    private static final int MIN_LENGTH = 16;
    /** 연결 서문의 고정 앞부분. 뒤 두 바이트는 예약이라 값이 달라도 서문이다. */
    private static final String PREFACE = "c0de535452";

    /** 형식이 깨진 바이트를 일부러 넣어 프레임으로 끝까지 풀리지 않는 벡터의 `파일명/id`. */
    private static final Set<String> INTENTIONALLY_MALFORMED = new TreeSet<>(Arrays.asList(
            "close-races/length-error-after-own-no-error-close-closes-without-resend",
            "close-races/close-length-below-minimum-closes-silently",
            "hello-establishment/server-first-frame-length-below-minimum-closes-silently",
            "hello-establishment/preface-version-mismatch-closes-silently",
            "hello-establishment/preface-http-request-closes-silently",
            "websocket/preface-seven-bytes-closes-silently",
            "websocket/preface-nine-bytes-closes-silently",
            "websocket/first-frame-shorter-than-header-closes-silently",
            "websocket/first-frame-size-mismatch-closes-with-protocol-error",
            "websocket/message-longer-than-frame-closes",
            "websocket/message-shorter-than-frame-closes",
            "websocket/close-message-size-mismatch-closes-silently"));

    private static List<String> hexes(JsonObject vector) {
        List<String> result = new ArrayList<>();
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            JsonObject s = step.getAsJsonObject();
            if (s.has("hex")) {
                result.add(Bytes.compactHex(s.get("hex").getAsString()));
            }
        }
        return result;
    }

    /** 앞의 프레임부터 풀어 20바이트 헤더, length 16 이상, 프레임 끝이 입력 안에 있는지 본다. 어긋나면 이유를 돌려준다. */
    private static String decodeProblem(String hex) {
        byte[] bytes = Bytes.fromHex(hex);
        int pos = hex.startsWith(PREFACE) ? 8 : 0;
        while (pos < bytes.length) {
            if (bytes.length - pos < HEADER_SIZE) {
                return "헤더(20바이트)보다 짧은 꼬리 " + (bytes.length - pos) + "바이트";
            }
            long length = Bytes.readUnsignedInt(bytes, pos);
            if (length < MIN_LENGTH) {
                return "length " + length + " < 16";
            }
            if (pos + 4 + length > bytes.length) {
                return "length " + length + "가 입력 끝을 넘음";
            }
            pos += (int) (4 + length);
        }
        return null;
    }

    @Test
    void everyFrameInEveryVectorDecodesWithA20ByteHeader() {
        for (Path file : RawVectors.files()) {
            for (JsonObject vector : RawVectors.vectors(file)) {
                String key = VectorSuite.fileKey(file) + "/" + vector.get("id").getAsString();
                if (INTENTIONALLY_MALFORMED.contains(key)) {
                    continue;
                }
                for (String hex : hexes(vector)) {
                    assertThat(decodeProblem(hex)).as(key).isNull();
                }
            }
        }
    }

    @Test
    void intentionallyMalformedVectorsExist() {
        assertThat(RawVectors.byKey().keySet()).containsAll(INTENTIONALLY_MALFORMED);
    }

    @Test
    void streamIdsAreWithin63Bits() {
        for (JsonObject vector : RawVectors.byKey().values()) {
            assertThat(vector.toString()).doesNotContain("2147483647", "2147483646", "2^31");
        }
    }

    @Test
    void largestStreamIdIs2Pow63Minus1() {
        JsonObject largest = RawVectors.byKey().get("streams/request-with-largest-stream-id-is-accepted");
        // 20바이트 헤더에서 stream_id는 8~15바이트(16진 문자 16~31)다.
        assertThat(hexes(largest).get(0).substring(16, 32)).isEqualTo("7fffffffffffffff");
    }
}
