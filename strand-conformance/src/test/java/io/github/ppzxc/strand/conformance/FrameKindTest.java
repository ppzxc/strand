package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** 프레임 종류 정의의 내부 불변식: 기대값 렌더링과 실제 바이트 렌더링이 같은 정의에서 나온다. */
class FrameKindTest {

    /** 프레임 종류마다 기대 JSON 표본. 이름 있는 종류는 하나도 빠지지 않아야 한다. */
    private static final Map<String, String[]> SAMPLES = new LinkedHashMap<>();

    static {
        SAMPLES.put("PREFACE", new String[] {"{\"type\":\"PREFACE\"}"});
        SAMPLES.put("HELLO", new String[] {
            "{\"type\":\"HELLO\"}",
            "{\"type\":\"HELLO\",\"params\":{\"max_frame_size\":1024,\"capabilities\":3}}",
            "{\"type\":\"HELLO\",\"flags\":[\"ACK\"]}"
        });
        SAMPLES.put("PING", new String[] {
            "{\"type\":\"PING\",\"payload\":\"0102030405060708\"}",
            "{\"type\":\"PING\",\"flags\":[\"ACK\"],\"payload\":\"0102030405060708\"}"
        });
        SAMPLES.put("AUTH", new String[] {
            "{\"type\":\"AUTH\",\"mechanism\":\"ANONYMOUS\"}",
            "{\"type\":\"AUTH\",\"mechanism\":\"TOKEN\",\"credentials\":\"0a0b\"}",
            "{\"type\":\"AUTH\",\"flags\":[\"ACK\"]}",
            "{\"type\":\"AUTH\",\"flags\":[\"ACK\"],\"params\":{\"credential_lifetime\":3600}}"
        });
        SAMPLES.put("CLOSE", new String[] {
            "{\"type\":\"CLOSE\",\"error_code\":\"PROTOCOL_ERROR\",\"last_stream_id\":\"7\"}",
            "{\"type\":\"CLOSE\",\"error_code\":300,\"last_stream_id\":\"18446744073709551615\"}"
        });
        SAMPLES.put("RESET", new String[] {"{\"type\":\"RESET\",\"stream_id\":\"3\",\"error_code\":\"CANCEL\"}"});
        SAMPLES.put("WEBSOCKET_CLOSE", new String[] {"{\"type\":\"WEBSOCKET_CLOSE\",\"code\":1000}"});
    }

    private static JsonObject json(String text) {
        return JsonParser.parseString(text).getAsJsonObject();
    }

    @Test
    void everyNamedFrameKindHasASample() {
        assertThat(SAMPLES.keySet())
                .containsExactlyInAnyOrderElementsOf(FrameKind.shapesByName().keySet());
    }

    @Test
    void renderingTheExpectedJsonEqualsRenderingItsEncodedBytes() {
        for (Map.Entry<String, String[]> kind : SAMPLES.entrySet()) {
            if (kind.getKey().equals("WEBSOCKET_CLOSE")) {
                continue; // 바이트가 없는 종류는 아래 시험이 본다.
            }
            for (String sample : kind.getValue()) {
                JsonObject frame = json(sample);
                assertThat(FrameKind.describe(
                                FrameKind.encode(frame), kind.getKey().equals("PREFACE"), true))
                        .as(sample)
                        .containsExactly(FrameKind.expected(frame));
            }
        }
    }

    @Test
    void anApplicationFrameRendersTheSameFromJsonAndBytes() {
        // 클라이언트 역할에서 홀수 stream_id는 자기 요청이라 status를 그리지 않고, 짝수는 상대 응답이라 그린다.
        String request = "{\"type\":256,\"stream_id\":\"1\",\"payload\":\"abcd\"}";
        String response = "{\"type\":300,\"stream_id\":\"2\",\"status\":\"NOT_FOUND\",\"payload\":\"\"}";
        String reserved = "{\"type\":256,\"stream_id\":\"4\",\"status\":99}";
        for (String sample : new String[] {request, response, reserved}) {
            JsonObject frame = json(sample);
            assertThat(FrameKind.describe(FrameKind.encode(frame), false, true))
                    .as(sample)
                    .containsExactly(FrameKind.expected(frame));
        }
    }

    @Test
    void theServerRoleSkipsStatusOnEvenStreamsAndShowsItOnOddOnes() {
        JsonObject own = json("{\"type\":256,\"stream_id\":\"2\",\"payload\":\"\"}");
        JsonObject peers = json("{\"type\":256,\"stream_id\":\"1\",\"status\":\"OK\",\"payload\":\"\"}");

        assertThat(FrameKind.describe(FrameKind.encode(own), false, false)).containsExactly(FrameKind.expected(own));
        assertThat(FrameKind.describe(FrameKind.encode(peers), false, false))
                .containsExactly(FrameKind.expected(peers));
    }

    @Test
    void renderingKeepsTheDocumentedFormatForFixedFrames() {
        assertThat(FrameKind.expected(
                        json("{\"type\":\"AUTH\",\"flags\":[\"ACK\"],\"params\":{\"credential_lifetime\":9}}")))
                .isEqualTo("AUTH flags=ACK stream_id=0 params=credential_lifetime=9");
        assertThat(FrameKind.expected(
                        json("{\"type\":\"CLOSE\",\"error_code\":\"NO_ERROR\",\"last_stream_id\":\"5\"}")))
                .isEqualTo("CLOSE flags=- stream_id=0 error_code=NO_ERROR last_stream_id=5");
        // 확장 영역과 ACK 밖의 flags는 실제 바이트에서만 나타난다.
        byte[] withExtensions =
                Bytes.fromHex("00000012" + "0002" + "02" + "00" + "0000000000000000" + "0002" + "0000" + "abcd" + "");
        assertThat(FrameKind.describe(withExtensions, false, true))
                .containsExactly("PING flags=0x02 stream_id=0 extensions=2 payload=");
    }

    @Test
    void aWebSocketCloseHasNoBytesAndRendersFromItsCode() {
        JsonObject close = json(SAMPLES.get("WEBSOCKET_CLOSE")[0]);

        assertThat(FrameKind.webSocketClose(1000)).isEqualTo(FrameKind.expected(close));
        assertThatThrownBy(() -> FrameKind.encode(close)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aFrameOfNoKnownKindRendersItsRawTypeNumber() {
        byte[] bytes = Bytes.fromHex("00000010" + "0010" + "00" + "00" + "0000000000000000" + "00000000");

        assertThat(FrameKind.describe(bytes, false, true)).containsExactly("type=0x0010 flags=- stream_id=0 payload=");
    }
}
