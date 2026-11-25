package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.gson.JsonArray;
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
import java.util.function.IntPredicate;
import java.util.regex.Pattern;

final class FrameKind {

    private static final class Fields extends LinkedHashMap<String, String> {
        private static final long serialVersionUID = 1L;

        Fields with(String name, String value) {
            put(name, value);
            return this;
        }
    }

    private static final class Wire {
        final int type;
        final int flags;
        final int status;
        final int reserved;
        final long streamId;
        final int extensions;
        final byte[] payload;

        Wire(byte[] frame) {
            this.type = Bytes.readUnsignedShort(frame, Protocol.TYPE_AT);
            this.flags = frame[Protocol.FLAGS_AT] & 0xFF;
            this.status = frame[Protocol.STATUS_AT] & 0xFF;
            this.reserved = Bytes.readUnsignedShort(frame, Protocol.RESERVED_AT);
            this.streamId = Bytes.readLong(frame, Protocol.STREAM_ID_AT);
            this.extensions = Bytes.readUnsignedShort(frame, Protocol.EXTENSIONS_LENGTH_AT);
            assertThat(Protocol.HEADER_SIZE + extensions)
                    .as("extensions_length가 프레임 안에 있어야 한다")
                    .isLessThanOrEqualTo(frame.length);
            this.payload = Arrays.copyOfRange(frame, Protocol.HEADER_SIZE + extensions, frame.length);
        }

        boolean ack() {
            return (flags & Protocol.ACK_FLAG) != 0;
        }

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

    private static final byte[] PREFACE_BYTES = Bytes.fromHex(Protocol.PREFACE_HEX);

    private static final String PREFACE = "PREFACE";
    private static final String WEBSOCKET_CLOSE = "WEBSOCKET_CLOSE";
    private static final String APP = "APP";

    private static final FrameKind PREFACE_KIND = new FrameKind(
            PREFACE, null, FRAME_BASE, frame -> new Fields(), frame -> PREFACE_BYTES.clone(), (wire, client) -> null);

    private static final FrameKind HELLO_KIND = new FrameKind(
            "HELLO",
            typeIs(Protocol.TYPE_HELLO),
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
            typeIs(Protocol.TYPE_PING),
            FRAME_BASE
                    .require("payload", Check::pingPayload)
                    .allow("flags", FieldCheck.flags(VectorSchema.ACK_FLAGS))
                    .allow("stream_id", VectorSchema.ZERO_STREAM_ID),
            frame -> controlHead(VectorSchema.hasFlag(frame, "ACK"))
                    .with("payload", frame.get("payload").getAsString()),
            frame -> control(
                    Protocol.TYPE_PING,
                    VectorSchema.hasFlag(frame, "ACK"),
                    Bytes.fromHex(frame.get("payload").getAsString())),
            (wire, client) -> wire.head().with("payload", Bytes.toHex(wire.payload)));

    private static final FrameKind AUTH_KIND = new FrameKind(
            "AUTH",
            typeIs(Protocol.TYPE_AUTH),
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
                    return control(Protocol.TYPE_AUTH, true, authAckPayload(frame));
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
            typeIs(Protocol.TYPE_CLOSE),
            FRAME_BASE
                    .require("error_code", VectorSchema.ERROR_CODE)
                    .require("last_stream_id", VectorSchema.LAST_STREAM_ID)
                    .allow("stream_id", VectorSchema.ZERO_STREAM_ID)
                    .allow("reason", Check::string),
            frame -> {
                Fields fields = controlHead(false)
                        .with("error_code", Protocol.nameOfError(frame.get("error_code")))
                        .with("last_stream_id", frame.get("last_stream_id").getAsString());
                if (frame.has("reason")) {
                    fields.with("reason", Bytes.toHex(reasonBytes(frame)));
                }
                return fields;
            },
            frame -> {
                ByteArrayOutputStream payload = new ByteArrayOutputStream();
                Bytes.writeInt(payload, Protocol.errorCode(frame.get("error_code")));
                Bytes.writeLong(
                        payload,
                        Long.parseUnsignedLong(frame.get("last_stream_id").getAsString()));
                byte[] reason = reasonBytes(frame);
                payload.write(reason, 0, reason.length);
                return control(Protocol.TYPE_CLOSE, false, payload.toByteArray());
            },
            (wire, client) -> wire.payload.length < Protocol.CLOSE_PAYLOAD_SIZE
                    ? null
                    : wire.head()
                            .with("error_code", Protocol.nameOfError((int) Bytes.readUnsignedInt(wire.payload, 0)))
                            .with("last_stream_id", Long.toUnsignedString(Bytes.readLong(wire.payload, 4)))
                            .with(
                                    "reason",
                                    Bytes.toHex(Arrays.copyOfRange(
                                            wire.payload, Protocol.CLOSE_PAYLOAD_SIZE, wire.payload.length))));

    private static final FrameKind RESET_KIND = new FrameKind(
            "RESET",
            typeIs(Protocol.TYPE_RESET),
            FRAME_BASE
                    .require("stream_id", VectorSchema.POSITIVE_STREAM_ID)
                    .require("error_code", VectorSchema.ERROR_CODE),
            frame -> new Fields()
                    .with("flags", flagsText(0))
                    .with("stream_id", String.valueOf(Protocol.streamId(frame.get("stream_id"))))
                    .with("error_code", Protocol.nameOfError(frame.get("error_code"))),
            frame -> {
                ByteArrayOutputStream payload = new ByteArrayOutputStream();
                Bytes.writeInt(payload, Protocol.errorCode(frame.get("error_code")));
                return Bytes.concat(
                        header(Protocol.TYPE_RESET, 0, 0, Protocol.streamId(frame.get("stream_id")), payload.size()),
                        payload.toByteArray());
            },
            (wire, client) -> wire.payload.length != Protocol.RESET_PAYLOAD_SIZE
                    ? null
                    : wire.head()
                            .with("error_code", Protocol.nameOfError((int) Bytes.readUnsignedInt(wire.payload, 0))));

    private static final FrameKind WEBSOCKET_CLOSE_KIND = new FrameKind(
            WEBSOCKET_CLOSE,
            null,
            FRAME_BASE.require("code", VectorSchema.WEBSOCKET_CLOSE_CODE),
            frame -> webSocketCloseFields(frame.get("code").getAsInt()),
            null,
            (wire, client) -> null);

    private static final FrameKind APPLICATION_KIND = new FrameKind(
            APP,
            type -> type >= Protocol.FIRST_APPLICATION_TYPE,
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
            frame -> {
                byte[] payload = Bytes.fromHex(
                        frame.has("payload") ? frame.get("payload").getAsString() : "");
                int status = frame.has("status") ? Protocol.statusCode(frame.get("status")) : 0;
                return Bytes.concat(
                        header(
                                frame.get("type").getAsInt(),
                                0,
                                status,
                                Protocol.streamId(frame.get("stream_id")),
                                payload.length),
                        payload);
            },
            (wire, client) -> {
                Fields fields = new Fields().with("type", String.valueOf(wire.type));
                fields.putAll(wire.head());
                if (isResponse(wire, client)) {
                    fields.with("status", Protocol.nameOfStatus(wire.status));
                }
                return fields.with("payload", Bytes.toHex(wire.payload));
            });

    private static final List<FrameKind> NAMED = Collections.unmodifiableList(Arrays.asList(
            PREFACE_KIND, HELLO_KIND, PING_KIND, AUTH_KIND, CLOSE_KIND, RESET_KIND, WEBSOCKET_CLOSE_KIND));

    private static final List<FrameKind> DECODING;

    private static final Map<String, Shape> SHAPES_BY_NAME;

    private static final Map<String, FrameKind> KINDS_BY_NAME;

    static {
        List<FrameKind> decoding = new ArrayList<>();
        Map<String, Shape> shapes = new LinkedHashMap<>();
        Map<String, FrameKind> kinds = new LinkedHashMap<>();
        for (FrameKind kind : NAMED) {
            shapes.put(kind.name, kind.shape);
            kinds.put(kind.name, kind);
            if (kind.onWire != null) {
                decoding.add(kind);
            }
        }
        decoding.add(APPLICATION_KIND);
        DECODING = Collections.unmodifiableList(decoding);
        SHAPES_BY_NAME = Collections.unmodifiableMap(shapes);
        KINDS_BY_NAME = Collections.unmodifiableMap(kinds);
    }

    private final String name;

    private final IntPredicate onWire;

    private final Shape shape;
    private final Function<JsonObject, Fields> expected;
    private final Function<JsonObject, byte[]> encoder;
    private final BiFunction<Wire, Boolean, Fields> decoder;

    private FrameKind(
            String name,
            IntPredicate onWire,
            Shape shape,
            Function<JsonObject, Fields> expected,
            Function<JsonObject, byte[]> encoder,
            BiFunction<Wire, Boolean, Fields> decoder) {
        this.name = name;
        this.onWire = onWire;
        this.shape = shape;
        this.expected = expected;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    private static IntPredicate typeIs(int type) {
        return actual -> actual == type;
    }

    static Map<String, Shape> shapesByName() {
        return SHAPES_BY_NAME;
    }

    static Shape applicationShape() {
        return APPLICATION_KIND.shape;
    }

    static boolean isSupported(JsonObject frame) {
        String type = VectorSchema.frameType(frame);
        if (type != null) {
            return byName(type) != null;
        }
        return (!frame.has("extensions") || frame.getAsJsonArray("extensions").size() == 0)
                && APPLICATION_KIND.onWire.test(frame.get("type").getAsInt());
    }

    static boolean hasSupportedFlags(JsonObject frame) {
        return !frame.has("flags") || frame.getAsJsonArray("flags").size() == 0 || VectorSchema.hasFlag(frame, "ACK");
    }

    private static FrameKind byName(String name) {
        return KINDS_BY_NAME.get(name);
    }

    private static FrameKind of(JsonObject frame) {
        String type = VectorSchema.frameType(frame);
        if (type == null) {
            return APPLICATION_KIND;
        }
        FrameKind kind = byName(type);
        if (kind == null) {
            throw new IllegalArgumentException("알 수 없는 프레임 종류: " + type);
        }
        return kind;
    }

    static String expected(JsonObject frame) {
        FrameKind kind = of(frame);
        return format(kind.name, kind.expected.apply(frame));
    }

    static byte[] encode(JsonObject frame) {
        FrameKind kind = of(frame);
        if (kind.encoder == null) {
            throw new IllegalArgumentException(kind.name + "은 바이트로 인코딩하지 않는다");
        }
        return kind.encoder.apply(frame);
    }

    static String webSocketClose(int code) {
        return format(WEBSOCKET_CLOSE, webSocketCloseFields(code));
    }

    private static Fields webSocketCloseFields(int code) {
        return new Fields().with("code", String.valueOf(code));
    }

    static List<String> describe(byte[] output, boolean startsWithPreface, boolean client) {
        List<String> result = new ArrayList<>();
        byte[] rest = output;
        byte[] preface = PREFACE_BYTES;
        if (startsWithPreface
                && rest.length >= preface.length
                && Arrays.equals(Arrays.copyOf(rest, preface.length), preface)) {
            result.add(format(PREFACE_KIND.name, new Fields()));
            rest = Arrays.copyOfRange(rest, preface.length, rest.length);
        }
        for (byte[] frame : split(rest)) {
            Wire wire = new Wire(frame);
            result.add(describeFrame(wire, client));
        }
        return result;
    }

    private static boolean isResponse(Wire wire, boolean client) {
        return (wire.streamId & 1) != (client ? 1 : 0);
    }

    private static void markNonZero(Fields fields, Wire wire, boolean response) {
        if (wire.reserved != 0) {
            fields.with("reserved", String.format("0x%04x", wire.reserved));
        }
        if (wire.status != 0 && !response) {
            fields.with("sent_status", String.valueOf(wire.status));
        }
    }

    private static byte[] reasonBytes(JsonObject close) {
        return close.has("reason") ? close.get("reason").getAsString().getBytes(StandardCharsets.UTF_8) : new byte[0];
    }

    private static String describeFrame(Wire wire, boolean client) {
        for (FrameKind kind : DECODING) {
            if (kind.onWire.test(wire.type)) {
                Fields fields = kind.decoder.apply(wire, client);
                if (fields != null) {
                    markNonZero(fields, wire, kind == APPLICATION_KIND && isResponse(wire, client));
                    return format(kind.name, fields);
                }
            }
        }

        Fields unknown = wire.head().with("payload", Bytes.toHex(wire.payload));
        markNonZero(unknown, wire, false);
        return format(String.format("type=0x%04x", wire.type), unknown);
    }

    private static final Pattern REASON_FIELD = Pattern.compile(" reason=[0-9a-f]*");

    static List<String> comparable(List<String> actual, JsonArray expectedSend) {
        List<String> result = new ArrayList<>(actual);
        for (int i = 0; i < result.size() && i < expectedSend.size(); i++) {
            JsonObject expected = expectedSend.get(i).getAsJsonObject();
            if ("CLOSE".equals(VectorSchema.frameType(expected)) && !expected.has("reason")) {
                result.set(i, REASON_FIELD.matcher(result.get(i)).replaceFirst(""));
            }
        }
        return result;
    }

    private static List<byte[]> split(byte[] output) {
        List<byte[]> result = new ArrayList<>();
        int at = 0;
        while (at + Protocol.LENGTH_SIZE <= output.length) {
            long length = Bytes.readUnsignedInt(output, at);
            assertThat(length)
                    .as("프레임 length는 %d 이상이어야 한다", Protocol.MIN_FRAME_LENGTH)
                    .isGreaterThanOrEqualTo(Protocol.MIN_FRAME_LENGTH);
            assertThat(at + Protocol.LENGTH_SIZE + length)
                    .as("보낸 바이트가 프레임 경계에서 끝나야 한다")
                    .isLessThanOrEqualTo(output.length);
            int size = (int) (Protocol.LENGTH_SIZE + length);
            result.add(Arrays.copyOfRange(output, at, at + size));
            at += size;
        }
        assertThat(at).as("보낸 바이트에 남는 조각이 없어야 한다").isEqualTo(output.length);
        return result;
    }

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
            int id = Bytes.readUnsignedShort(payload, at);
            String name = Protocol.HELLO_PARAM_NAMES.containsKey(id)
                    ? Protocol.HELLO_PARAM_NAMES.get(id)
                    : String.format("0x%04x", id);
            filled.put(name, Bytes.readUnsignedInt(payload, at + 2));
        }
        return paramsText(filled);
    }

    private static String authParamsText(byte[] payload) {
        if (payload.length == 0) {
            return "-";
        }
        StringBuilder text = new StringBuilder();
        for (int at = 0; at + Protocol.PARAM_ENTRY_SIZE <= payload.length; at += Protocol.PARAM_ENTRY_SIZE) {
            int id = Bytes.readUnsignedShort(payload, at);
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

    private static byte[] helloPayload(JsonObject frame) {
        Map<String, Long> params = Protocol.helloParams(frame.getAsJsonObject("params"));
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        for (Map.Entry<Integer, String> param : Protocol.HELLO_PARAM_NAMES.entrySet()) {
            Bytes.writeShort(payload, param.getKey());
            Bytes.writeInt(payload, params.get(param.getValue()));
        }
        return payload.toByteArray();
    }

    private static byte[] authAckPayload(JsonObject frame) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        if (frame.has("params") && frame.getAsJsonObject("params").has("credential_lifetime")) {
            Bytes.writeShort(payload, Protocol.AUTH_PARAM_CREDENTIAL_LIFETIME);
            Bytes.writeInt(
                    payload,
                    frame.getAsJsonObject("params").get("credential_lifetime").getAsLong());
        }
        return payload.toByteArray();
    }

    private static byte[] control(int type, boolean ack, byte[] payload) {
        return Bytes.concat(header(type, ack ? Protocol.ACK_FLAG : 0, 0, 0L, payload.length), payload);
    }

    private static byte[] header(int type, int flags, int status, long streamId, int payloadSize) {
        ByteArrayOutputStream header = new ByteArrayOutputStream();
        Bytes.writeInt(header, Protocol.HEADER_SIZE - Protocol.LENGTH_SIZE + payloadSize);
        Bytes.writeShort(header, type);
        header.write(flags);
        header.write(status);
        Bytes.writeLong(header, streamId);
        Bytes.writeShort(header, 0);
        Bytes.writeShort(header, 0);
        return header.toByteArray();
    }
}
