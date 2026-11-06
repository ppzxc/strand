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
import java.util.Locale;
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
    private static final String ACK_HELLO_HEX = "0000000c000101000000000000000000";
    private static final Map<String, Long> HELLO_DEFAULTS = new LinkedHashMap<>();
    private static final Map<Integer, String> HELLO_PARAMS = new LinkedHashMap<>();
    private static final Set<String> SUPPORTED_SETUP =
            new HashSet<>(Arrays.asList("role", "established", "local_hello", "peer_hello", "config"));
    private static final Set<String> SUPPORTED_STEPS =
            new HashSet<>(Arrays.asList("recv", "advance", "ping", "request", "close"));
    private static final Set<String> SUPPORTED_FRAMES =
            new HashSet<>(Arrays.asList("PREFACE", "HELLO", "PING", "CLOSE"));
    private static final Set<String> SUPPORTED_EVENTS = new HashSet<>(Arrays.asList("closed", "result"));
    private static final long DEFAULT_ACK_TIMEOUT_MS = 5000L;
    private static final int FIRST_APPLICATION_TYPE = 256;

    static {
        HELLO_DEFAULTS.put("max_frame_size", 4_194_304L);
        HELLO_DEFAULTS.put("max_concurrent_streams", 100L);
        HELLO_DEFAULTS.put("capabilities", 0L);
        HELLO_PARAMS.put(1, "max_frame_size");
        HELLO_PARAMS.put(2, "max_concurrent_streams");
        HELLO_PARAMS.put(3, "capabilities");
    }

    private final boolean client;
    private final Endpoint endpoint;

    private VectorRunner(boolean client, Endpoint endpoint) {
        this.client = client;
        this.endpoint = endpoint;
    }

    static void run(ConformanceVector vector) {
        JsonObject setup = vector.setup();
        requireSupported(vector, setup);

        boolean client = setup.get("role").getAsString().equals("client");
        EndpointConfig config = configOf(setup);
        VectorRunner runner = new VectorRunner(client, client ? Endpoint.client(config) : Endpoint.server(config));
        // 클라이언트 역할 구현은 연결이 열리자마자 서문과 HELLO를 내보낸다. 수립하지 않는 벡터에서는 첫 단계의 send에 합쳐 비교한다.
        byte[] opening = runner.endpoint.drainOutput();
        if (setup.get("established").getAsBoolean()) {
            runner.establish(opening, setup.getAsJsonObject("peer_hello"));
            opening = new byte[0];
        }
        int number = 0;
        for (JsonObject step : vector.steps()) {
            number++;
            runner.apply(step);
            byte[] sent = Bytes.concat(opening, runner.endpoint.drainOutput());
            boolean startsWithPreface = opening.length > 0;
            opening = new byte[0];
            assertThat(runner.describe(sent, startsWithPreface))
                    .as("%d번째 단계의 send", number)
                    .isEqualTo(expectedFrames(step));
            assertThat(describe(runner.endpoint.drainEvents()))
                    .as("%d번째 단계의 events", number)
                    .isEqualTo(expectedEvents(step));
        }
    }

    /** 실행하지 못하는 부분이 있으면 벡터를 건너뛴다. 한 단계도 실행하기 전에 판정한다. */
    private static void requireSupported(ConformanceVector vector, JsonObject setup) {
        for (String key : setup.keySet()) {
            skipUnless(SUPPORTED_SETUP.contains(key), vector, "setup." + key);
        }
        if (setup.has("config")) {
            for (String key : setup.getAsJsonObject("config").keySet()) {
                skipUnless(key.equals("ack_timeout_ms"), vector, "config." + key);
            }
        }
        for (JsonObject step : vector.steps()) {
            String kind = step.get("kind").getAsString();
            skipUnless(SUPPORTED_STEPS.contains(kind), vector, "단계 " + kind);
            skipUnless(
                    !kind.equals("request")
                            || !step.has("extensions")
                            || step.getAsJsonArray("extensions").size() == 0,
                    vector,
                    "request.extensions");
            for (JsonElement element : step.getAsJsonArray("send")) {
                JsonObject frame = element.getAsJsonObject();
                String type = VectorSchema.frameType(frame);
                skipUnless(
                        type != null ? SUPPORTED_FRAMES.contains(type) : isSupportedApplicationFrame(frame),
                        vector,
                        "프레임 " + frame.get("type"));
                skipUnless(
                        !frame.has("flags")
                                || frame.getAsJsonArray("flags").size() == 0
                                || VectorSchema.hasFlag(frame, "ACK"),
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

    /** 숫자 type의 애플리케이션 프레임 가운데 아직 비교할 수 있는 것: status·확장 영역을 쓰지 않은 프레임. */
    private static boolean isSupportedApplicationFrame(JsonObject frame) {
        return !frame.has("status")
                && (!frame.has("extensions")
                        || frame.getAsJsonArray("extensions").size() == 0)
                && frame.get("type").getAsInt() >= FIRST_APPLICATION_TYPE;
    }

    private static void skipUnless(boolean supported, ConformanceVector vector, String what) {
        Assumptions.assumeTrue(supported, "러너 미지원: " + what + " (" + vector.id() + ")");
    }

    private static EndpointConfig configOf(JsonObject setup) {
        Map<String, Long> hello = helloParams(setup.getAsJsonObject("local_hello"));
        long ackTimeout = setup.has("config") && setup.getAsJsonObject("config").has("ack_timeout_ms")
                ? setup.getAsJsonObject("config").get("ack_timeout_ms").getAsLong()
                : DEFAULT_ACK_TIMEOUT_MS;
        return EndpointConfig.defaults()
                .maxFrameSize(hello.get("max_frame_size"))
                .maxConcurrentStreams(hello.get("max_concurrent_streams"))
                .capabilities(hello.get("capabilities"))
                .ackTimeoutMs(ackTimeout);
    }

    /** 규격 3.3절 표준 수립 순서를 실행하고, 성공을 확인한 뒤 출력과 이벤트를 비운다. */
    private void establish(byte[] opening, JsonObject peerHello) {
        byte[] hello = opening;
        if (!client) {
            endpoint.receive(Bytes.fromHex(PREFACE_HEX));
            hello = endpoint.drainOutput();
        }
        assertThat(describe(hello, client)).as("수립: 연결을 연 뒤의 출력").hasSize(client ? 2 : 1);
        endpoint.receive(encodeHello(helloParams(peerHello)));
        assertThat(describe(endpoint.drainOutput(), false))
                .as("수립: 상대 HELLO에 대한 ACK")
                .containsExactly("HELLO flags=ACK stream_id=0 params=-");
        endpoint.receive(Bytes.fromHex(ACK_HELLO_HEX));
        assertThat(endpoint.drainOutput()).as("수립: ACK 뒤의 출력").isEmpty();
        assertThat(endpoint.drainEvents()).as("수립: 이벤트").isEmpty();
    }

    private void apply(JsonObject step) {
        switch (step.get("kind").getAsString()) {
            case "recv":
                endpoint.receive(Bytes.fromHex(step.get("hex").getAsString()));
                break;
            case "advance":
                endpoint.advance(step.get("ms").getAsLong());
                break;
            case "ping":
                endpoint.ping(Bytes.fromHex(step.get("payload").getAsString()));
                break;
            case "request":
                endpoint.request(
                        step.get("type").getAsInt(),
                        Bytes.fromHex(step.has("payload") ? step.get("payload").getAsString() : ""));
                break;
            default:
                endpoint.close(errorCode(step.get("error_code")));
        }
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
            String type = VectorSchema.frameType(frame);
            boolean ack = VectorSchema.hasFlag(frame, "ACK");
            String flags = ack ? "ACK" : "-";
            if (type == null) {
                result.add("APP type=" + frame.get("type").getAsInt() + " flags=- stream_id="
                        + frame.get("stream_id").getAsInt() + " payload="
                        + (frame.has("payload") ? frame.get("payload").getAsString() : ""));
            } else if (type.equals("PREFACE")) {
                result.add("PREFACE");
            } else if (type.equals("HELLO")) {
                result.add("HELLO flags=" + flags + " stream_id=0 params="
                        + (ack ? "-" : paramsText(helloParams(frame.getAsJsonObject("params")))));
            } else if (type.equals("PING")) {
                result.add("PING flags=" + flags + " stream_id=0 payload="
                        + frame.get("payload").getAsString());
            } else {
                result.add("CLOSE flags=- stream_id=0 error_code=" + nameOfError(frame.get("error_code")));
            }
        }
        return result;
    }

    private static List<String> expectedEvents(JsonObject step) {
        List<String> result = new ArrayList<>();
        for (JsonElement element : step.getAsJsonArray("events")) {
            JsonObject event = element.getAsJsonObject();
            if (event.get("event").getAsString().equals("result")) {
                result.add("result stream_id=" + event.get("stream_id").getAsInt() + " status="
                        + nameOfStatus(event.get("status")));
            } else {
                result.add("closed");
            }
        }
        return result;
    }

    private static List<String> describe(List<Event> events) {
        List<String> result = new ArrayList<>();
        for (Event event : events) {
            if (event.kind() == Event.Kind.RESULT) {
                result.add("result stream_id=" + event.streamId() + " status=" + nameOfStatus(event.status()));
            } else {
                result.add(event.kind().name().toLowerCase(Locale.ROOT));
            }
        }
        return result;
    }

    /** 엔드포인트가 보낸 바이트를 프레임으로 풀어 벡터의 `send`와 같은 표기로 바꾼다. 잘린 프레임은 실패시킨다. */
    private List<String> describe(byte[] output, boolean startsWithPreface) {
        List<String> result = new ArrayList<>();
        byte[] rest = output;
        byte[] preface = Bytes.fromHex(PREFACE_HEX);
        if (startsWithPreface
                && rest.length >= preface.length
                && Arrays.equals(Arrays.copyOf(rest, preface.length), preface)) {
            result.add("PREFACE");
            rest = Arrays.copyOfRange(rest, preface.length, rest.length);
        }
        for (byte[] frame : frames(rest)) {
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
                result.add(
                        "CLOSE" + head + ext + " error_code=" + nameOfError((int) Bytes.readUnsignedInt(payload, 0)));
            } else if (type >= FIRST_APPLICATION_TYPE) {
                // 자기 번호 공간(클라이언트는 홀수, 서버는 짝수)의 프레임은 요청이라 status를 비교하지 않는다.
                boolean ownSpace = streamId % 2 == (client ? 1 : 0);
                String status = ownSpace ? "" : " status=" + (frame[7] & 0xFF);
                result.add("APP type=" + type + head + ext + status + " payload=" + Bytes.toHex(payload));
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
        Map<String, Long> filled = new LinkedHashMap<>(HELLO_DEFAULTS);
        for (int at = 0; at + 6 <= payload.length; at += 6) {
            int id = ((payload[at] & 0xFF) << 8) | (payload[at + 1] & 0xFF);
            String name = HELLO_PARAMS.containsKey(id) ? HELLO_PARAMS.get(id) : String.format("0x%04x", id);
            filled.put(name, Bytes.readUnsignedInt(payload, at + 2));
        }
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

    /** error_code 이름(예약 값은 정수)을 값으로. */
    private static int errorCode(JsonElement element) {
        return element.getAsJsonPrimitive().isString()
                ? FieldKind.ERROR_CODE_NAMES.indexOf(element.getAsString())
                : element.getAsInt();
    }

    private static String nameOfError(JsonElement element) {
        return nameOfError(errorCode(element));
    }

    private static String nameOfError(int code) {
        return code < FieldKind.ERROR_CODE_NAMES.size() ? FieldKind.ERROR_CODE_NAMES.get(code) : String.valueOf(code);
    }

    private static String nameOfStatus(JsonElement element) {
        return element.getAsJsonPrimitive().isString() ? element.getAsString() : nameOfStatus(element.getAsInt());
    }

    private static String nameOfStatus(int status) {
        return status < FieldKind.STATUS_NAMES.size() ? FieldKind.STATUS_NAMES.get(status) : String.valueOf(status);
    }
}
