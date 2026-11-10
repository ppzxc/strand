package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonObject;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 프레임 종류 하나의 정의. 종류마다 기대 JSON의 형태, 기대 JSON의 렌더링, 기대 JSON의 바이트 인코딩, 실제 바이트의 디코딩을 한 곳에
 * 모은다. 이름 있는 프레임(PREFACE, HELLO, PING, AUTH, CLOSE, RESET, WEBSOCKET_CLOSE)과 숫자 type 애플리케이션 프레임이 대상이다.
 *
 * <p>기대값 렌더링과 실제 바이트 렌더링은 모두 {@link #format}에서 나온다. 형태 검사 표와 러너의 프레임 표는 이 정의에서 파생된다.
 */
final class FrameKind {

    /** 렌더링 모델: 필드 이름에서 값 문자열로. 기대 JSON과 실제 바이트가 모두 이 모양으로 모인 뒤 {@link #format}으로 그려진다. */
    private static final class Fields extends LinkedHashMap<String, String> {
        private static final long serialVersionUID = 1L;

        Fields with(String name, String value) {
            put(name, value);
            return this;
        }
    }

    /** 디코딩한 프레임 하나. */
    private static final class Wire {
        final int type;
        final int flags;
        final int status;
        final long streamId;
        final int extensions;
        final byte[] payload;

        Wire(byte[] frame) {
            this.type = ((frame[Protocol.TYPE_AT] & 0xFF) << 8) | (frame[Protocol.TYPE_AT + 1] & 0xFF);
            this.flags = frame[Protocol.FLAGS_AT] & 0xFF;
            this.status = frame[Protocol.STATUS_AT] & 0xFF;
            this.streamId = Bytes.readLong(frame, Protocol.STREAM_ID_AT);
            this.extensions = ((frame[Protocol.EXTENSIONS_LENGTH_AT] & 0xFF) << 8)
                    | (frame[Protocol.EXTENSIONS_LENGTH_AT + 1] & 0xFF);
            this.payload = Arrays.copyOfRange(frame, Protocol.HEADER_SIZE + extensions, frame.length);
        }

        boolean ack() {
            return (flags & Protocol.ACK_FLAG) != 0;
        }

        /** flags, stream_id, 있으면 extensions. */
        Fields head() {
            Fields fields =
                    new Fields().with("flags", flagsText(flags)).with("stream_id", Long.toUnsignedString(streamId));
            if (extensions != 0) {
                fields.with("extensions", String.valueOf(extensions));
            }
            return fields;
        }
    }

    private static final Shape FRAME_BASE = Shape.EMPTY.require(VectorSchema.FRAME_TAG, FieldCheck.DEFERRED);

    private static final String PREFACE = "PREFACE";
    private static final String WEBSOCKET_CLOSE = "WEBSOCKET_CLOSE";
    private static final String APP = "APP";

    private static final FrameKind PREFACE_KIND = new FrameKind(
            PREFACE,
            -1,
            FRAME_BASE,
            frame -> new Fields(),
            frame -> Bytes.fromHex(Protocol.PREFACE_HEX),
            (wire, client) -> null);

    private static final FrameKind HELLO_KIND = new FrameKind(
            "HELLO",
            Protocol.TYPE_HELLO,
            FRAME_BASE
                    .allow("flags", FieldCheck.flags(VectorSchema.ACK_FLAGS))
                    .allow("stream_id", VectorSchema.ZERO_STREAM_ID)
                    .allow("params", FieldCheck.object(VectorSchema.HELLO_PARAMS)),
            frame -> {
                boolean ack = VectorSchema.hasFlag(frame, "ACK");
                return controlHead(ack)
                        .with("params", ack ? "-" : paramsText(Protocol.helloParams(frame.getAsJsonObject("params"))));
            },
            frame -> {
                boolean ack = VectorSchema.hasFlag(frame, "ACK");
                return control(Protocol.TYPE_HELLO, ack, ack ? new byte[0] : helloPayload(frame));
            },
            (wire, client) -> wire.head().with("params", wire.payload.length == 0 ? "-" : paramsText(wire.payload)));

    private static final FrameKind PING_KIND = new FrameKind(
            "PING",
            Protocol.TYPE_PING,
            FRAME_BASE
                    .require("payload", Check::pingPayload)
                    .allow("flags", FieldCheck.flags(VectorSchema.ACK_FLAGS))
                    .allow("stream_id", VectorSchema.ZERO_STREAM_ID),
            frame -> controlHead(VectorSchema.hasFlag(frame, "ACK"))
                    .with("payload", frame.get("payload").getAsString()),
            null,
            (wire, client) -> wire.head().with("payload", Bytes.toHex(wire.payload)));

    private static final FrameKind AUTH_KIND = new FrameKind(
            "AUTH",
            Protocol.TYPE_AUTH,
            FRAME_BASE
                    .allow("flags", FieldCheck.flags(VectorSchema.ACK_FLAGS))
                    .allow("stream_id", VectorSchema.ZERO_STREAM_ID)
                    .allow("mechanism", Check::mechanism)
                    .allow("credentials", Check::hex)
                    .allow("params", FieldCheck.object(VectorSchema.AUTH_PARAMS)),
            frame -> {
                if (VectorSchema.hasFlag(frame, "ACK")) {
                    JsonObject params = frame.has("params") ? frame.getAsJsonObject("params") : null;
                    return controlHead(true)
                            .with(
                                    "params",
                                    params != null && params.has("credential_lifetime")
                                            ? "credential_lifetime="
                                                    + params.get("credential_lifetime")
                                                            .getAsLong()
                                            : "-");
                }
                return controlHead(false)
                        .with("mechanism", frame.get("mechanism").getAsString())
                        .with(
                                "credentials",
                                frame.has("credentials")
                                        ? frame.get("credentials").getAsString()
                                        : "");
            },
            frame -> {
                if (VectorSchema.hasFlag(frame, "ACK")) {
                    return control(Protocol.TYPE_AUTH, true, new byte[0]);
                }
                byte[] name = frame.get("mechanism").getAsString().getBytes(StandardCharsets.US_ASCII);
                byte[] credentials = Bytes.fromHex(
                        frame.has("credentials") ? frame.get("credentials").getAsString() : "");
                return control(
                        Protocol.TYPE_AUTH,
                        false,
                        Bytes.concat(Bytes.concat(new byte[] {(byte) name.length}, name), credentials));
            },
            (wire, client) -> {
                if (wire.ack()) {
                    return wire.head().with("params", authParamsText(wire.payload));
                }
                if (wire.payload.length < 1) {
                    return null;
                }
                int nameLength = wire.payload[0] & 0xFF;
                return wire.head()
                        .with("mechanism", new String(wire.payload, 1, nameLength, StandardCharsets.US_ASCII))
                        .with(
                                "credentials",
                                Bytes.toHex(Arrays.copyOfRange(wire.payload, 1 + nameLength, wire.payload.length)));
            });

    private static final FrameKind CLOSE_KIND = new FrameKind(
            "CLOSE",
            Protocol.TYPE_CLOSE,
            FRAME_BASE
                    .require("error_code", VectorSchema.ERROR_CODE)
                    .require("last_stream_id", VectorSchema.LAST_STREAM_ID)
                    .allow("stream_id", VectorSchema.ZERO_STREAM_ID),
            frame -> controlHead(false)
                    .with("error_code", Protocol.nameOfError(frame.get("error_code")))
                    .with("last_stream_id", frame.get("last_stream_id").getAsString()),
            null,
            (wire, client) -> wire.payload.length < Protocol.CLOSE_PAYLOAD_SIZE
                    ? null
                    : wire.head()
                            .with("error_code", Protocol.nameOfError((int) Bytes.readUnsignedInt(wire.payload, 0)))
                            .with("last_stream_id", Long.toUnsignedString(Bytes.readLong(wire.payload, 4))));

    private static final FrameKind RESET_KIND = new FrameKind(
            "RESET",
            Protocol.TYPE_RESET,
            FRAME_BASE
                    .require("stream_id", VectorSchema.POSITIVE_STREAM_ID)
                    .require("error_code", VectorSchema.ERROR_CODE),
            frame -> new Fields()
                    .with("flags", flagsText(0))
                    .with("stream_id", String.valueOf(Protocol.streamId(frame.get("stream_id"))))
                    .with("error_code", Protocol.nameOfError(frame.get("error_code"))),
            null,
            (wire, client) -> wire.payload.length != Protocol.RESET_PAYLOAD_SIZE
                    ? null
                    : wire.head()
                            .with("error_code", Protocol.nameOfError((int) Bytes.readUnsignedInt(wire.payload, 0))));

    /** WebSocket Close는 바이트가 아니라 전송 메시지다. 인코딩·디코딩할 바이트가 없다. */
    private static final FrameKind WEBSOCKET_CLOSE_KIND = new FrameKind(
            WEBSOCKET_CLOSE,
            -1,
            FRAME_BASE.require("code", VectorSchema.WEBSOCKET_CLOSE_CODE),
            frame -> new Fields().with("code", String.valueOf(frame.get("code").getAsInt())),
            null,
            (wire, client) -> null);

    /** 숫자 type이 256 이상인 애플리케이션 프레임. 확장 영역은 쓰지 않는다. */
    private static final FrameKind APPLICATION_KIND = new FrameKind(
            APP,
            -1,
            FRAME_BASE
                    .require("stream_id", VectorSchema.STREAM_ID)
                    .allow("status", VectorSchema.STATUS)
                    .allow("extensions", Check::extensions)
                    .allow("payload", Check::hex),
            frame -> {
                Fields fields = new Fields()
                        .with("type", String.valueOf(frame.get("type").getAsInt()))
                        .with("flags", flagsText(0))
                        .with("stream_id", String.valueOf(Protocol.streamId(frame.get("stream_id"))));
                if (frame.has("status")) {
                    fields.with("status", Protocol.nameOfStatus(frame.get("status")));
                }
                return fields.with(
                        "payload", frame.has("payload") ? frame.get("payload").getAsString() : "");
            },
            null,
            (wire, client) -> {
                Fields fields = new Fields().with("type", String.valueOf(wire.type));
                fields.putAll(wire.head());
                // 자기 번호 공간(클라이언트는 홀수, 서버는 짝수)의 프레임은 요청이라 status를 비교하지 않는다.
                boolean ownSpace = (wire.streamId & 1) == (client ? 1 : 0);
                if (!ownSpace) {
                    fields.with("status", Protocol.nameOfStatus(wire.status));
                }
                return fields.with("payload", Bytes.toHex(wire.payload));
            });

    /** 이름 있는 프레임 종류. 형태 검사 표와 러너의 프레임 표가 여기서 나온다. */
    private static final List<FrameKind> NAMED = Collections.unmodifiableList(Arrays.asList(
            PREFACE_KIND, HELLO_KIND, PING_KIND, AUTH_KIND, CLOSE_KIND, RESET_KIND, WEBSOCKET_CLOSE_KIND));

    private final String name;
    private final int type;
    private final Shape shape;
    private final Function<JsonObject, Fields> expected;
    private final Function<JsonObject, byte[]> encoder;
    private final BiFunction<Wire, Boolean, Fields> decoder;

    private FrameKind(
            String name,
            int type,
            Shape shape,
            Function<JsonObject, Fields> expected,
            Function<JsonObject, byte[]> encoder,
            BiFunction<Wire, Boolean, Fields> decoder) {
        this.name = name;
        this.type = type;
        this.shape = shape;
        this.expected = expected;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    // ---- 파생: 형태 검사 표, 지원 여부 ----

    /** 이름 있는 프레임의 이름에서 형태로. */
    static Map<String, Shape> shapesByName() {
        Map<String, Shape> shapes = new LinkedHashMap<>();
        for (FrameKind kind : NAMED) {
            shapes.put(kind.name, kind.shape);
        }
        return shapes;
    }

    /** 숫자 type 애플리케이션 프레임의 형태. */
    static Shape applicationShape() {
        return APPLICATION_KIND.shape;
    }

    /** 기대 프레임이 비교할 수 있는 종류인가: 이름 있는 프레임이거나, 확장 영역 없는 애플리케이션 프레임. */
    static boolean isSupported(JsonObject frame) {
        String type = VectorSchema.frameType(frame);
        if (type != null) {
            return byName(type) != null;
        }
        return (!frame.has("extensions") || frame.getAsJsonArray("extensions").size() == 0)
                && frame.get("type").getAsInt() >= Protocol.FIRST_APPLICATION_TYPE;
    }

    /** 기대 프레임의 flags를 비교할 수 있는가: 비었거나 ACK뿐. */
    static boolean hasSupportedFlags(JsonObject frame) {
        return !frame.has("flags") || frame.getAsJsonArray("flags").size() == 0 || VectorSchema.hasFlag(frame, "ACK");
    }

    private static FrameKind byName(String name) {
        for (FrameKind kind : NAMED) {
            if (kind.name.equals(name)) {
                return kind;
            }
        }
        return null;
    }

    private static FrameKind of(JsonObject frame) {
        String type = VectorSchema.frameType(frame);
        return type == null ? APPLICATION_KIND : byName(type);
    }

    // ---- 기대 JSON: 렌더링과 인코딩 ----

    /** 기대 프레임을 벡터 {@code send}의 표기로. */
    static String expected(JsonObject frame) {
        FrameKind kind = of(frame);
        return format(kind.name, kind.expected.apply(frame));
    }

    /** 기대 프레임을 바이트로. WEBSOCKET_CLOSE는 바이트가 없어 지원하지 않는다. */
    static byte[] encode(JsonObject frame) {
        FrameKind kind = of(frame);
        if (kind.encoder == null) {
            throw new IllegalArgumentException(kind.name + "은 바이트로 인코딩하지 않는다");
        }
        return kind.encoder.apply(frame);
    }

    /** 엔드포인트가 낸 WebSocket Close 메시지의 표기. 기대 항목과 같은 정의에서 나온다. */
    static String webSocketClose(int code) {
        JsonObject frame = new JsonObject();
        frame.addProperty("type", WEBSOCKET_CLOSE);
        frame.addProperty("code", code);
        return expected(frame);
    }

    // ---- 실제 바이트: 디코딩과 렌더링 ----

    /**
     * 엔드포인트가 보낸 바이트를 프레임으로 풀어 벡터의 {@code send}와 같은 표기로 바꾼다. 잘린 프레임은 실패시킨다. {@code client}는
     * 이 쪽 역할로, 애플리케이션 프레임의 status 비교 여부를 정한다.
     */
    static List<String> describe(byte[] output, boolean startsWithPreface, boolean client) {
        List<String> result = new ArrayList<>();
        byte[] rest = output;
        byte[] preface = PREFACE_KIND.encoder.apply(null);
        if (startsWithPreface
                && rest.length >= preface.length
                && Arrays.equals(Arrays.copyOf(rest, preface.length), preface)) {
            result.add(format(PREFACE_KIND.name, new Fields()));
            rest = Arrays.copyOfRange(rest, preface.length, rest.length);
        }
        for (byte[] frame : split(rest)) {
            result.add(describeFrame(new Wire(frame), client));
        }
        return result;
    }

    private static String describeFrame(Wire wire, boolean client) {
        for (FrameKind kind : NAMED) {
            if (kind.type == wire.type) {
                Fields fields = kind.decoder.apply(wire, client);
                if (fields != null) {
                    return format(kind.name, fields);
                }
            }
        }
        if (wire.type >= Protocol.FIRST_APPLICATION_TYPE) {
            return format(APPLICATION_KIND.name, APPLICATION_KIND.decoder.apply(wire, client));
        }
        return format(String.format("type=0x%04x", wire.type), wire.head().with("payload", Bytes.toHex(wire.payload)));
    }

    private static List<byte[]> split(byte[] output) {
        List<byte[]> result = new ArrayList<>();
        int at = 0;
        while (at + Protocol.LENGTH_SIZE <= output.length) {
            int size = (int) (Protocol.LENGTH_SIZE + Bytes.readUnsignedInt(output, at));
            assertThat(at + size).as("보낸 바이트가 프레임 경계에서 끝나야 한다").isLessThanOrEqualTo(output.length);
            result.add(Arrays.copyOfRange(output, at, at + size));
            at += size;
        }
        assertThat(at).as("보낸 바이트에 남는 조각이 없어야 한다").isEqualTo(output.length);
        return result;
    }

    // ---- 렌더링 형식: 기대값과 실제 바이트가 함께 쓰는 유일한 형식 ----

    private static String format(String name, Fields fields) {
        StringBuilder text = new StringBuilder(name);
        for (Map.Entry<String, String> field : fields.entrySet()) {
            text.append(' ').append(field.getKey()).append('=').append(field.getValue());
        }
        return text.toString();
    }

    private static String flagsText(int flags) {
        if (flags == 0) {
            return "-";
        }
        return flags == Protocol.ACK_FLAG ? "ACK" : String.format("0x%02x", flags);
    }

    /** 기대 제어 프레임의 머리말: flags와 0으로 고정된 stream_id. */
    private static Fields controlHead(boolean ack) {
        return new Fields()
                .with("flags", flagsText(ack ? Protocol.ACK_FLAG : 0))
                .with("stream_id", "0");
    }

    private static String paramsText(Map<String, Long> params) {
        return params.toString();
    }

    private static String paramsText(byte[] payload) {
        Map<String, Long> filled = new LinkedHashMap<>(Protocol.HELLO_DEFAULTS);
        for (int at = 0; at + Protocol.PARAM_ENTRY_SIZE <= payload.length; at += Protocol.PARAM_ENTRY_SIZE) {
            int id = ((payload[at] & 0xFF) << 8) | (payload[at + 1] & 0xFF);
            String name = Protocol.HELLO_PARAM_NAMES.containsKey(id)
                    ? Protocol.HELLO_PARAM_NAMES.get(id)
                    : String.format("0x%04x", id);
            filled.put(name, Bytes.readUnsignedInt(payload, at + 2));
        }
        return paramsText(filled);
    }

    /** AUTH(ACK) payload의 항목. {@code credential_lifetime}만 이름이 있고 빈 payload는 {@code -}다. */
    private static String authParamsText(byte[] payload) {
        if (payload.length == 0) {
            return "-";
        }
        StringBuilder text = new StringBuilder();
        for (int at = 0; at + Protocol.PARAM_ENTRY_SIZE <= payload.length; at += Protocol.PARAM_ENTRY_SIZE) {
            int id = ((payload[at] & 0xFF) << 8) | (payload[at + 1] & 0xFF);
            text.append(text.length() == 0 ? "" : ",")
                    .append(
                            id == Protocol.AUTH_PARAM_CREDENTIAL_LIFETIME
                                    ? "credential_lifetime"
                                    : String.format("0x%04x", id))
                    .append('=')
                    .append(Bytes.readUnsignedInt(payload, at + 2));
        }
        return text.toString();
    }

    // ---- 인코딩 도구 ----

    private static byte[] helloPayload(JsonObject frame) {
        Map<String, Long> params = Protocol.helloParams(frame.getAsJsonObject("params"));
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        for (Map.Entry<Integer, String> param : Protocol.HELLO_PARAM_NAMES.entrySet()) {
            writeShort(payload, param.getKey());
            writeInt(payload, params.get(param.getValue()));
        }
        return payload.toByteArray();
    }

    /** 확장 영역이 없고 {@code status}와 {@code stream_id}가 0인 제어 프레임(규격 2.1절). */
    private static byte[] control(int type, boolean ack, byte[] payload) {
        return Bytes.concat(header(type, ack ? Protocol.ACK_FLAG : 0, 0, 0L, payload.length), payload);
    }

    /** 확장 영역이 없는 프레임의 헤더 20바이트(규격 2.1절). {@code length}는 header 뒤 payload 크기로 채운다. */
    private static byte[] header(int type, int flags, int status, long streamId, int payloadSize) {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        writeInt(header, Protocol.HEADER_SIZE - Protocol.LENGTH_SIZE + payloadSize);
        writeShort(header, type);
        header.write(flags);
        header.write(status);
        writeLong(header, streamId);
        writeShort(header, 0);
        writeShort(header, 0);
        return header.toByteArray();
    }

    private static void writeShort(ByteArrayOutputStream out, int value) {
        out.write(value >>> 8);
        out.write(value);
    }

    private static void writeInt(ByteArrayOutputStream out, long value) {
        for (int shift = 24; shift >= 0; shift -= 8) {
            out.write((int) (value >>> shift));
        }
    }

    private static void writeLong(ByteArrayOutputStream out, long value) {
        writeInt(out, value >>> 32);
        writeInt(out, value);
    }
}
