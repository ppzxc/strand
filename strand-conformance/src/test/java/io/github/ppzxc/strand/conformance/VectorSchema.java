package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class VectorSchema {

    static final int KNOWN_FORMAT = 2;

    static final List<String> ROLES = Arrays.asList("client", "server");
    static final String TCP = "tcp";
    static final String WEBSOCKET = "websocket";
    static final String DEFAULT_TRANSPORT = TCP;
    static final List<String> TRANSPORTS = Arrays.asList(TCP, WEBSOCKET);
    private static final List<String> LOCAL_RESULT_STATUSES =
            Arrays.asList("UNAVAILABLE", "CANCELLED", "DEADLINE_EXCEEDED");
    static final List<String> CERTAINTIES = Arrays.asList("response", "not_processed", "unknown");
    static final List<String> OPCODES = Arrays.asList("binary", "text");
    static final List<String> ACK_FLAGS = Arrays.asList("ACK");

    private static final long U8_MAX = 255L;
    private static final long U16_MAX = 65535L;
    private static final long U32_MAX = 4294967295L;
    private static final BigInteger U64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private static final BigInteger STREAM_ID_MAX = BigInteger.ONE.shiftLeft(63).subtract(BigInteger.ONE);

    private static final FieldCheck U16 = FieldCheck.range(0, U16_MAX);
    private static final FieldCheck U32 = FieldCheck.range(0, U32_MAX);

    static final FieldCheck POSITIVE_U32 = FieldCheck.range(1, U32_MAX);

    static final FieldCheck APP_TYPE = FieldCheck.range(Protocol.FIRST_APPLICATION_TYPE, U16_MAX);

    static final FieldCheck STATUS = FieldCheck.named(Protocol.STATUS_NAMES, U8_MAX);

    static final FieldCheck ERROR_CODE = FieldCheck.named(Protocol.ERROR_CODE_NAMES, U32_MAX);

    static final FieldCheck STREAM_ID = FieldCheck.decimal(BigInteger.ZERO, STREAM_ID_MAX);

    static final FieldCheck LAST_STREAM_ID = FieldCheck.decimal(BigInteger.ZERO, U64_MAX);

    static final FieldCheck POSITIVE_STREAM_ID = FieldCheck.decimal(BigInteger.ONE, STREAM_ID_MAX);

    static final FieldCheck ZERO_STREAM_ID = FieldCheck.decimal(BigInteger.ZERO, BigInteger.ZERO);

    static final FieldCheck WEBSOCKET_CLOSE_CODE = FieldCheck.range(Protocol.NORMAL_CLOSURE, Protocol.NORMAL_CLOSURE);

    private static final FieldCheck FORMAT = (c, path, element) -> c.format(path, element, KNOWN_FORMAT);

    static final Shape FILE = Shape.EMPTY.require("format", FORMAT).require("vectors", Check::array);

    static final Shape VECTOR = Shape.EMPTY
            .require("id", Check::id)
            .require("description", Check::nonEmptyText)
            .require("requirements", Check::requirementIds)
            .require("setup", VectorSchema::setup)
            .require("steps", VectorSchema::steps);

    static final Shape EXTENSION_ENTRY = Shape.EMPTY.require("tag", U16).require("value", Check::hex);

    static final Shape HELLO_PARAMS = helloShape();

    private static Shape helloShape() {
        Shape shape = Shape.EMPTY;
        for (String name : Protocol.HELLO_DEFAULTS.keySet()) {
            shape = shape.allow(name, U32);
        }
        return shape;
    }

    static final Shape SETUP_CONFIG = configShape();

    private static Shape configShape() {
        Shape shape = Shape.EMPTY;
        for (String key : Protocol.CONFIG_DEFAULTS.keySet()) {
            shape = shape.allow(key, POSITIVE_U32);
        }
        return shape;
    }

    static List<String> setupKeys() {
        return new ArrayList<>(SETUP.names());
    }

    static String hexOrEmpty(JsonObject object, String field) {
        return object.has(field) ? object.get(field).getAsString() : "";
    }

    static long requestFrameSize(JsonObject request) {
        long size = Protocol.HEADER_SIZE + (long) Bytes.fromHex(hexOrEmpty(request, "payload")).length;
        if (request.has("extensions")) {
            JsonArray extensions = request.getAsJsonArray("extensions");
            for (int i = 0; i < extensions.size(); i++) {
                JsonObject extension = extensions.get(i).getAsJsonObject();
                size += 4L + Bytes.fromHex(extension.get("value").getAsString()).length;
            }
        }
        return size;
    }

    static final Shape AUTH_SETUP = Shape.EMPTY
            .allow("mechanism", Check::mechanism)
            .allow("credentials", Check::hex)
            .allow("decision", FieldCheck.oneOf(Arrays.asList("transient_failure")))
            .allow("lifetime_seconds", U32);

    static final Shape AUTH_PARAMS = Shape.EMPTY.allow("credential_lifetime", U32);

    private static final Shape SETUP = Shape.EMPTY
            .require("role", FieldCheck.oneOf(ROLES))
            .require("established", Check::bool)
            .allow("local_hello", FieldCheck.object(HELLO_PARAMS))
            .allow("peer_hello", FieldCheck.object(HELLO_PARAMS))
            .allow("config", FieldCheck.object(SETUP_CONFIG))
            .allow("auth", FieldCheck.object(AUTH_SETUP))
            .allow("transport", FieldCheck.oneOf(TRANSPORTS))
            .allow("next_stream_id", POSITIVE_STREAM_ID)
            .allow("known_types", Check::uniqueAppTypes);

    static final String FRAME_TAG = "type";
    private static final String FRAME_LABEL = "프레임 type";

    private static final String EVENT_LABEL = EventKind.TAG;

    private VectorSchema() {}

    static void setup(Check c, String path, JsonElement element) {
        JsonObject setup = c.object(path, element, SETUP);
        if (setup.has("auth")
                && (setup.getAsJsonObject("auth").has("decision")
                        || setup.getAsJsonObject("auth").has("lifetime_seconds"))
                && "client".equals(setup.get("role").getAsString())) {
            JsonObject auth = setup.getAsJsonObject("auth");
            String field = auth.has("decision") ? "decision" : "lifetime_seconds";
            throw c.failAt(Check.child(Check.child(path, "auth"), field), "서버 인증 설정은 서버 역할에서만 쓸 수 있다");
        }
        if (setup.has("next_stream_id")) {
            boolean client = "client".equals(setup.get("role").getAsString());
            BigInteger next = new BigInteger(setup.get("next_stream_id").getAsString());
            if (next.testBit(0) != client) {
                throw c.failAt(
                        Check.child(path, "next_stream_id"),
                        "role이 " + (client ? "client이면 홀수" : "server이면 짝수") + "여야 한다: " + next);
            }
        }
    }

    static void crossCheck(Check c, JsonObject vector) {
        JsonObject setup = vector.getAsJsonObject("setup");
        String transport = setup.has("transport") ? setup.get("transport").getAsString() : DEFAULT_TRANSPORT;
        JsonArray steps = vector.getAsJsonArray("steps");
        boolean server = "server".equals(setup.get("role").getAsString());
        boolean[] authAckReceived = AuthGate.ackReceivedThrough(vector);
        boolean authGateOpen = setup.get("established").getAsBoolean();
        Set<String> requestNames = new HashSet<>();
        for (int i = 0; i < steps.size(); i++) {
            JsonObject step = steps.get(i).getAsJsonObject();
            StepKind kind = StepKind.of(step);
            if (StepKind.REQUEST_NAME.equals(kind.name()) && step.has("write_limit_bytes")) {
                if (step.get("write_limit_bytes").getAsLong() >= requestFrameSize(step)) {
                    throw c.failAt(
                            Check.child(Check.item("steps", i), "write_limit_bytes"), "부분 쓰기 한도는 요청 프레임 크기보다 작아야 한다");
                }
            }
            if (StepKind.REQUEST_NAME.equals(kind.name()) && step.has("name")) {
                String name = step.get("name").getAsString();
                if (name.trim().isEmpty() || !requestNames.add(name)) {
                    throw c.failAt(Check.child(Check.item("steps", i), "name"), "요청 이름은 비어 있지 않고 벡터 안에서 유일해야 한다");
                }
            }
            if (StepKind.CANCEL_NAME.equals(kind.name())) {
                boolean byStreamId = step.has("stream_id");
                boolean byName = step.has("request");
                if (byStreamId == byName) {
                    throw c.failAt(
                            Check.child(Check.item("steps", i), "kind"), "cancel은 stream_id 또는 request 이름 하나로 지정한다");
                }
                if (byName && !requestNames.contains(step.get("request").getAsString())) {
                    throw c.failAt(Check.child(Check.item("steps", i), "request"), "앞에서 선언한 request 이름이어야 한다");
                }
            }
            if (kind.isCredentialNotification() && (!server || !authGateOpen)) {
                throw c.failAt(
                        Check.child(Check.item("steps", i), StepKind.TAG), "자격 증명 통지는 서버 역할의 AUTH 관문 통과 뒤에만 쓸 수 있다");
            }
            if (kind.onlyOnTransport() != null && !kind.onlyOnTransport().equals(transport)) {
                throw c.failAt(
                        Check.child(Check.item("steps", i), StepKind.TAG),
                        "transport가 " + transport + "인 벡터에는 " + kind.name() + " 단계를 쓸 수 없다");
            }

            if (kind.sendsPing() && !setup.get("established").getAsBoolean() && !authGateOpen) {
                throw c.failAt(
                        Check.child(Check.item("steps", i), StepKind.TAG),
                        "established가 false인 벡터는 AUTH 관문 통과(서버 역할은 send의 AUTH ACK, 클라이언트 역할은 recv한 AUTH ACK) 이전에 ping 단계를 쓸 수 없다");
            }
            if (transport.equals(TCP)) {
                rejectWebSocketClose(c, i, step);
            }
            authGateOpen |= server ? sendsAuthAck(step) : authAckReceived[i];
        }
    }

    private static void rejectWebSocketClose(Check c, int stepIndex, JsonObject step) {
        JsonArray send = step.getAsJsonArray("send");
        for (int j = 0; j < send.size(); j++) {
            if ("WEBSOCKET_CLOSE".equals(frameType(send.get(j).getAsJsonObject()))) {
                throw c.failAt(
                        Check.child(Check.item(Check.child(Check.item("steps", stepIndex), "send"), j), FRAME_TAG),
                        "transport가 tcp인 벡터에는 WEBSOCKET_CLOSE 항목을 쓸 수 없다");
            }
        }
    }

    private static boolean sendsAuthAck(JsonObject step) {
        for (JsonElement frame : step.getAsJsonArray("send")) {
            JsonObject f = frame.getAsJsonObject();
            if ("AUTH".equals(frameType(f)) && hasFlag(f, "ACK")) {
                return true;
            }
        }
        return false;
    }

    static String frameType(JsonObject frame) {
        JsonElement type = frame.get(FRAME_TAG);
        return type != null
                        && type.isJsonPrimitive()
                        && type.getAsJsonPrimitive().isString()
                ? type.getAsString()
                : null;
    }

    static boolean hasFlag(JsonObject frame, String flag) {
        JsonElement flags = frame.get("flags");
        return flags != null && flags.isJsonArray() && flags.getAsJsonArray().contains(new JsonPrimitive(flag));
    }

    static void localResult(Check c, String path, JsonObject event) {
        if (event.has("stream_id")) {
            return;
        }
        if (!LOCAL_RESULT_STATUSES.contains(event.get("status").getAsString())) {
            throw c.failAt(
                    Check.child(path, "status"),
                    "stream_id 없는 result의 status는 UNAVAILABLE, CANCELLED, DEADLINE_EXCEEDED 중 하나여야 한다");
        }
        if (!"not_processed".equals(event.get("certainty").getAsString())) {
            throw c.failAt(Check.child(path, "certainty"), "stream_id 없는 result의 certainty는 not_processed여야 한다");
        }
    }

    static void steps(Check c, String path, JsonElement element) {
        JsonArray array = c.array(path, element);
        if (array.isEmpty()) {
            throw c.failAt(path, "하나 이상이어야 한다");
        }
        for (int i = 0; i < array.size(); i++) {
            String stepPath = Check.item(path, i);
            c.objectNamedBy(stepPath, array.get(i), StepKind.TAG, StepKind.LABEL, StepKind.shapesByName());
        }
    }

    static void frames(Check c, String path, JsonElement element) {
        JsonArray array = c.array(path, element);
        for (int i = 0; i < array.size(); i++) {
            frame(c, Check.item(path, i), array.get(i));
        }
    }

    static void events(Check c, String path, JsonElement element) {
        JsonArray array = c.array(path, element);
        for (int i = 0; i < array.size(); i++) {
            String eventPath = Check.item(path, i);
            JsonObject event =
                    c.objectNamedBy(eventPath, array.get(i), EventKind.TAG, EVENT_LABEL, EventKind.shapesByName());
            if ("result".equals(event.get(EventKind.TAG).getAsString())) {
                localResult(c, eventPath, event);
            }
        }
    }

    private static void frame(Check c, String path, JsonElement element) {
        JsonElement type = c.tag(path, element, FRAME_TAG);
        String typePath = Check.child(path, FRAME_TAG);
        Shape shape;
        if (Check.isString(type)) {
            shape = c.shapeFor(typePath, FRAME_LABEL, FrameKind.shapesByName(), type.getAsString());
        } else {
            APP_TYPE.check(c, typePath, type);
            shape = FrameKind.applicationShape();
        }
        JsonObject frame = c.object(path, element, shape);
        if ("AUTH".equals(frameType(frame))) {
            authFrame(c, path, frame);
        }
    }

    private static void authFrame(Check c, String path, JsonObject frame) {
        if (hasFlag(frame, "ACK")) {
            for (String field : new String[] {"mechanism", "credentials"}) {
                if (frame.has(field)) {
                    throw c.failAt(Check.child(path, field), "AUTH(ACK) 항목에는 " + field + "를 쓸 수 없다(ACK이면 params만 쓴다)");
                }
            }
        } else {
            if (!frame.has("mechanism")) {
                throw c.failAt(path, "필수 필드 누락: mechanism (ACK가 아닌 AUTH 항목)");
            }
            if (frame.has("params")) {
                throw c.failAt(Check.child(path, "params"), "ACK가 아닌 AUTH 항목에는 params를 쓸 수 없다(params는 ACK일 때만 쓴다)");
            }
        }
    }
}
