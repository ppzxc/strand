package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.github.ppzxc.strand.core.internal.Endpoint;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import io.github.ppzxc.strand.core.internal.Event;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assumptions;

/**
 * 벡터 하나를 엔드포인트에 실행한다. `setup`으로 엔드포인트를 만들고 단계마다 입력을 넣어, 보낸 바이트를 프레임으로 풀어 `send`와,
 * 애플리케이션 이벤트를 `events`와 순서까지 비교한다(벡터 형식 문서 6절, 7절). 엔드포인트의 외부 동작만 본다.
 *
 * <p>아직 실행할 수 없는 설정·단계·프레임이 하나라도 있으면 아무것도 실행하지 않고 건너뛴다. 통과로 위장하지 않기 위해서다.
 */
final class VectorRunner {

    private static final String PREFACE_HEX = "c0de535452300000";
    private static final Map<String, Long> HELLO_DEFAULTS = new LinkedHashMap<>();
    private static final Map<Integer, String> HELLO_PARAMS = new LinkedHashMap<>();
    private static final String[] ERROR_CODES = {
        "NO_ERROR", "PROTOCOL_ERROR", "INTERNAL_ERROR", "REFUSED", "CANCEL", "OVERLOAD"
    };
    private static final Set<String> SUPPORTED_SETUP =
            new HashSet<>(Arrays.asList("role", "established", "local_hello", "peer_hello"));
    private static final Set<String> SUPPORTED_FRAMES = new HashSet<>(Arrays.asList("HELLO", "PING", "CLOSE"));
    private static final Set<String> SUPPORTED_EVENTS = new HashSet<>(Arrays.asList("closed"));

    static {
        HELLO_DEFAULTS.put("max_frame_size", 4_194_304L);
        HELLO_DEFAULTS.put("max_concurrent_streams", 100L);
        HELLO_DEFAULTS.put("capabilities", 0L);
        HELLO_PARAMS.put(1, "max_frame_size");
        HELLO_PARAMS.put(2, "max_concurrent_streams");
        HELLO_PARAMS.put(3, "capabilities");
    }

    private VectorRunner() {}

    static void run(ConformanceVector vector) {
        JsonObject setup = vector.setup();
        requireSupported(vector, setup);

        Endpoint endpoint = Endpoint.server(configOf(setup.getAsJsonObject("local_hello")));
        if (setup.get("established").getAsBoolean()) {
            establish(endpoint, setup.getAsJsonObject("peer_hello"));
        }
        int number = 0;
        for (JsonObject step : vector.steps()) {
            number++;
            endpoint.receive(Bytes.fromHex(step.get("hex").getAsString()));
            assertThat(describe(endpoint.drainOutput()))
                    .as("%d번째 단계의 send", number)
                    .isEqualTo(expectedFrames(step));
            assertThat(describe(endpoint.drainEvents()))
                    .as("%d번째 단계의 events", number)
                    .isEqualTo(expectedEvents(step));
        }
    }

    /** 실행하지 못하는 부분이 있으면 벡터를 건너뛴다. 한 단계도 실행하기 전에 판정한다. */
    private static void requireSupported(ConformanceVector vector, JsonObject setup) {
        for (String key : setup.keySet()) {
            skipUnless(SUPPORTED_SETUP.contains(key), vector, "setup." + key);
        }
        skipUnless(setup.get("role").getAsString().equals("server"), vector, "role=client");
        for (JsonObject step : vector.steps()) {
            String kind = step.get("kind").getAsString();
            skipUnless(kind.equals("recv"), vector, "단계 " + kind);
            for (JsonElement frame : step.getAsJsonArray("send")) {
                JsonObject f = frame.getAsJsonObject();
                skipUnless(SUPPORTED_FRAMES.contains(VectorSchema.frameType(f)), vector, "프레임 " + f.get("type"));
                skipUnless(
                        !f.has("flags") || f.getAsJsonArray("flags").size() == 0 || VectorSchema.hasFlag(f, "ACK"),
                        vector,
                        "프레임 flags");
            }
            for (JsonElement event : step.getAsJsonArray("events")) {
                skipUnless(
                        SUPPORTED_EVENTS.contains(
                                event.getAsJsonObject().get("event").getAsString()),
                        vector,
                        "이벤트 " + event);
            }
        }
    }

    private static void skipUnless(boolean supported, ConformanceVector vector, String what) {
        Assumptions.assumeTrue(supported, "러너 미지원: " + what + " (" + vector.id() + ")");
    }

    private static EndpointConfig configOf(JsonObject localHello) {
        Map<String, Long> hello = helloParams(localHello);
        return EndpointConfig.defaults()
                .maxFrameSize(hello.get("max_frame_size"))
                .maxConcurrentStreams(hello.get("max_concurrent_streams"))
                .capabilities(hello.get("capabilities"));
    }

    /** 규격 3.3절 표준 수립 순서를 실행하고, 성공을 확인한 뒤 출력과 이벤트를 비운다. */
    private static void establish(Endpoint endpoint, JsonObject peerHello) {
        endpoint.receive(Bytes.fromHex(PREFACE_HEX));
        assertThat(frames(endpoint.drainOutput())).as("수립: 서문에 대한 HELLO").hasSize(1);
        endpoint.receive(encodeHello(helloParams(peerHello)));
        assertThat(describe(endpoint.drainOutput()))
                .as("수립: 상대 HELLO에 대한 ACK")
                .containsExactly("HELLO flags=ACK stream_id=0 params=-");
        endpoint.receive(Bytes.fromHex("0000000c000101000000000000000000"));
        assertThat(endpoint.drainOutput()).as("수립: ACK 뒤의 출력").isEmpty();
        assertThat(endpoint.drainEvents()).as("수립: 이벤트").isEmpty();
    }

    /** 필드 이름에서 값으로, 빠진 값은 규격 기본값으로 채운다. 객체가 없으면 모두 기본값이다. */
    private static Map<String, Long> helloParams(JsonObject given) {
        Map<String, Long> params = new LinkedHashMap<>(HELLO_DEFAULTS);
        if (given != null) {
            for (String name : given.keySet()) {
                params.put(name, given.get(name).getAsLong());
            }
        }
        return params;
    }

    private static byte[] encodeHello(Map<String, Long> params) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        for (Map.Entry<Integer, String> param : HELLO_PARAMS.entrySet()) {
            long value = params.get(param.getValue());
            payload.write(0);
            payload.write(param.getKey());
            for (int shift = 24; shift >= 0; shift -= 8) {
                payload.write((int) (value >>> shift));
            }
        }
        byte[] body = payload.toByteArray();
        return Bytes.concat(Bytes.fromHex(String.format("%08x000100000000000000000000", 12 + body.length)), body);
    }

    private static List<String> expectedFrames(JsonObject step) {
        List<String> result = new ArrayList<>();
        for (JsonElement element : step.getAsJsonArray("send")) {
            JsonObject frame = element.getAsJsonObject();
            String type = frame.get("type").getAsString();
            boolean ack = VectorSchema.hasFlag(frame, "ACK");
            switch (type) {
                case "HELLO":
                    result.add("HELLO flags=" + (ack ? "ACK" : "-") + " stream_id=0 params="
                            + (ack ? "-" : paramsText(helloParams(frame.getAsJsonObject("params")))));
                    break;
                case "PING":
                    result.add("PING flags=" + (ack ? "ACK" : "-") + " stream_id=0 payload="
                            + frame.get("payload").getAsString());
                    break;
                default:
                    result.add("CLOSE flags=- stream_id=0 error_code="
                            + frame.get("error_code").getAsString());
            }
        }
        return result;
    }

    private static List<String> expectedEvents(JsonObject step) {
        List<String> result = new ArrayList<>();
        for (JsonElement event : step.getAsJsonArray("events")) {
            result.add(event.getAsJsonObject().get("event").getAsString());
        }
        return result;
    }

    private static List<String> describe(List<Event> events) {
        List<String> result = new ArrayList<>();
        for (Event event : events) {
            result.add(event.kind().name().toLowerCase(java.util.Locale.ROOT));
        }
        return result;
    }

    /** 엔드포인트가 보낸 바이트를 프레임으로 풀어 벡터의 `send`와 같은 표기로 바꾼다. 잘린 프레임은 그대로 표시해 실패시킨다. */
    private static List<String> describe(byte[] output) {
        List<String> result = new ArrayList<>();
        for (byte[] frame : frames(output)) {
            int type = ((frame[4] & 0xFF) << 8) | (frame[5] & 0xFF);
            int flags = frame[6] & 0xFF;
            long streamId = Bytes.readUnsignedInt(frame, 8);
            int extensions = ((frame[12] & 0xFF) << 8) | (frame[13] & 0xFF);
            byte[] payload = Arrays.copyOfRange(frame, 16 + extensions, frame.length);
            String head = " flags=" + flagsText(flags) + " stream_id=" + streamId;
            String ext = extensions == 0 ? "" : " extensions=" + extensions;
            if (type == 1) {
                result.add("HELLO" + head + ext + " params=" + (payload.length == 0 ? "-" : paramsText(payload)));
            } else if (type == 2) {
                result.add("PING" + head + ext + " payload=" + Bytes.toHex(payload));
            } else if (type == 3 && payload.length >= 4) {
                int code = (int) Bytes.readUnsignedInt(payload, 0);
                result.add(
                        "CLOSE" + head + ext + " error_code=" + (code < ERROR_CODES.length ? ERROR_CODES[code] : code));
            } else {
                result.add(String.format("type=0x%04x", type) + head + ext + " payload=" + Bytes.toHex(payload));
            }
        }
        return result;
    }

    private static String flagsText(int flags) {
        if (flags == 0) {
            return "-";
        }
        return flags == 1 ? "ACK" : String.format("0x%02x", flags);
    }

    private static String paramsText(Map<String, Long> params) {
        return params.toString();
    }

    private static String paramsText(byte[] payload) {
        Map<String, Long> params = new LinkedHashMap<>();
        for (int at = 0; at + 6 <= payload.length; at += 6) {
            int id = ((payload[at] & 0xFF) << 8) | (payload[at + 1] & 0xFF);
            String name = HELLO_PARAMS.containsKey(id) ? HELLO_PARAMS.get(id) : String.format("0x%04x", id);
            params.put(name, Bytes.readUnsignedInt(payload, at + 2));
        }
        Map<String, Long> filled = new LinkedHashMap<>(HELLO_DEFAULTS);
        filled.putAll(params);
        return paramsText(filled);
    }

    private static List<byte[]> frames(byte[] output) {
        List<byte[]> result = new ArrayList<>();
        int at = 0;
        while (at + 4 <= output.length) {
            int size = (int) (4 + Bytes.readUnsignedInt(output, at));
            assertThat(at + size).as("보낸 바이트가 프레임 경계에서 끝나야 한다").isLessThanOrEqualTo(output.length);
            result.add(Arrays.copyOfRange(output, at, at + size));
            at += size;
        }
        assertThat(at).as("보낸 바이트에 남는 조각이 없어야 한다").isEqualTo(output.length);
        return result;
    }
}
