package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 적합성 벡터 파일의 엄격 로더. 규격 부록 G의 규칙을 Gson으로 직접 구현하며, 하나라도 어기면
 * {@link InvalidVectorException}을 던진다. JSON Schema 파일({@code conformance/schema/})은 다른 언어 구현자를 위한
 * 보조 자료이고, 어긋나면 부록이 우선한다.
 *
 * <p>모든 오류는 {@code 파일명: [위치](id=...) 경로: 메시지} 형식이다.
 */
final class VectorLoader {

    /** 이 로더가 아는 형식 버전(G.2). */
    private static final int KNOWN_FORMAT = 1;

    private static final Pattern ID = Pattern.compile("[a-z0-9]+(-[a-z0-9]+)*");
    private static final Pattern SPEC_SECTION = Pattern.compile("(?:[0-9]+|[A-G])(?:\\.[0-9]+)*");
    private static final Pattern PING_PAYLOAD = Pattern.compile("[0-9a-f]{16}");

    private static final long U16_MAX = 65535L;
    private static final long U32_MAX = 4294967295L;

    private static final List<String> STATUS_NAMES = Arrays.asList(
            "OK",
            "CANCELLED",
            "UNKNOWN",
            "INVALID_ARGUMENT",
            "DEADLINE_EXCEEDED",
            "NOT_FOUND",
            "ALREADY_EXISTS",
            "PERMISSION_DENIED",
            "RESOURCE_EXHAUSTED",
            "FAILED_PRECONDITION",
            "ABORTED",
            "OUT_OF_RANGE",
            "UNIMPLEMENTED",
            "INTERNAL",
            "UNAVAILABLE",
            "DATA_LOSS",
            "UNAUTHENTICATED");
    private static final List<String> ERROR_CODE_NAMES = Arrays.asList(
            "NO_ERROR",
            "PROTOCOL_ERROR",
            "INTERNAL_ERROR",
            "FRAME_SIZE_ERROR",
            "HELLO_TIMEOUT",
            "STREAM_CLOSED",
            "REFUSED",
            "CANCEL",
            "OVERLOAD");

    private VectorLoader() {}

    static List<ConformanceVector> load(Path file) {
        try {
            return parse(file.getFileName().toString(), new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static List<ConformanceVector> parse(String fileName, String json) {
        if (json.trim().isEmpty()) {
            throw new Check(fileName, "").fail("", "파일이 비어 있다");
        }
        JsonElement root = readTree(fileName, json);
        Check top = new Check(fileName, "");
        JsonObject file = top.object("", root, fields("format", Kind.ANY, "vectors", Kind.ANY), fields());
        top.checkFormat(file.get("format"));
        JsonArray vectors = top.array("vectors", file.get("vectors"));

        List<ConformanceVector> result = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        int index = 0;
        for (JsonElement element : vectors) {
            ConformanceVector vector = parseVector(fileName, index, element);
            if (!ids.add(vector.id())) {
                throw new Check(fileName, location(index, vector.id())).fail("", "id 중복");
            }
            result.add(vector);
            index++;
        }
        return result;
    }

    /**
     * 엄격 모드 JsonReader로 트리를 만든다. Gson 트리 API는 한 객체 안의 같은 키를 조용히 덮어쓰므로
     * 객체를 직접 조립하면서 중복 키를 거부한다.
     */
    private static JsonElement readTree(String fileName, String json) {
        Check check = new Check(fileName, "");
        try (JsonReader reader = new JsonReader(new StringReader(json))) {
            reader.setStrictness(Strictness.STRICT);
            JsonElement root = readValue(check, reader);
            if (reader.peek() != JsonToken.END_DOCUMENT) {
                throw check.fail("", "JSON 문법 오류: 최상위 값 뒤에 내용이 더 있다");
            }
            return root;
        } catch (IOException | JsonParseException | IllegalStateException | NumberFormatException e) {
            throw check.fail("", "JSON 문법 오류: " + e.getMessage());
        }
    }

    private static JsonElement readValue(Check check, JsonReader reader) throws IOException {
        switch (reader.peek()) {
            case BEGIN_OBJECT:
                JsonObject object = new JsonObject();
                reader.beginObject();
                while (reader.hasNext()) {
                    String name = reader.nextName();
                    if (object.has(name)) {
                        throw check.fail("", "JSON 중복 키: " + name);
                    }
                    object.add(name, readValue(check, reader));
                }
                reader.endObject();
                return object;
            case BEGIN_ARRAY:
                JsonArray array = new JsonArray();
                reader.beginArray();
                while (reader.hasNext()) {
                    array.add(readValue(check, reader));
                }
                reader.endArray();
                return array;
            case STRING:
                return new JsonPrimitive(reader.nextString());
            case NUMBER:
                return new JsonPrimitive(new BigDecimal(reader.nextString()));
            case BOOLEAN:
                return new JsonPrimitive(reader.nextBoolean());
            case NULL:
                reader.nextNull();
                return JsonNull.INSTANCE;
            default:
                throw new IllegalStateException("예상하지 못한 토큰: " + reader.peek());
        }
    }

    private static String location(int index, String id) {
        return id.isEmpty() ? "[" + index + "]" : "[" + index + "](id=" + id + ")";
    }

    private static ConformanceVector parseVector(String fileName, int index, JsonElement element) {
        // id를 알 수 있으면 이후 모든 오류 메시지에 id를 넣는다.
        String knownId = "";
        if (element.isJsonObject() && isString(element.getAsJsonObject().get("id"))) {
            knownId = element.getAsJsonObject().get("id").getAsString();
        }
        Check c = new Check(fileName, location(index, knownId));
        JsonObject vector = c.object(
                "",
                element,
                fields("id", Kind.ANY, "description", Kind.ANY, "spec", Kind.ANY, "setup", Kind.ANY, "steps", Kind.ANY),
                fields());

        String id = c.string("id", vector.get("id"));
        if (!ID.matcher(id).matches()) {
            throw c.fail("id", "kebab-case([a-z0-9]+(-[a-z0-9]+)*)가 아니다: '" + id + "'");
        }
        String description = c.string("description", vector.get("description"));
        if (description.isEmpty()) {
            throw c.fail("description", "비어 있으면 안 된다");
        }
        List<String> spec = new ArrayList<>();
        JsonArray specArray = c.array("spec", vector.get("spec"));
        if (specArray.isEmpty()) {
            throw c.fail("spec", "비어 있으면 안 된다");
        }
        for (int i = 0; i < specArray.size(); i++) {
            String section = c.string("spec[" + i + "]", specArray.get(i));
            if (!SPEC_SECTION.matcher(section).matches()) {
                throw c.fail("spec[" + i + "]", "규격 절 번호가 아니다: '" + section + "'");
            }
            spec.add(section);
        }
        JsonObject setup = c.setup(vector.get("setup"));
        JsonArray stepArray = c.array("steps", vector.get("steps"));
        if (stepArray.isEmpty()) {
            throw c.fail("steps", "하나 이상이어야 한다");
        }
        List<JsonObject> steps = new ArrayList<>();
        for (int i = 0; i < stepArray.size(); i++) {
            steps.add(c.step("steps[" + i + "]", stepArray.get(i)));
        }
        return new ConformanceVector(id, description, spec, setup, steps);
    }

    private static boolean isString(JsonElement value) {
        return value != null
                && value.isJsonPrimitive()
                && value.getAsJsonPrimitive().isString();
    }

    private static Map<String, Kind> fields(Object... namesAndKinds) {
        Map<String, Kind> map = new LinkedHashMap<>();
        for (int i = 0; i < namesAndKinds.length; i += 2) {
            map.put((String) namesAndKinds[i], (Kind) namesAndKinds[i + 1]);
        }
        return map;
    }

    /** 필드 값의 종류. {@code ANY}는 호출한 쪽이 따로 검증한다. */
    private enum Kind {
        ANY,
        HEX,
        U16,
        U32,
        STREAM_ID,
        APP_TYPE,
        STATUS,
        ERROR_CODE,
        EXTENSIONS,
        HELLO_PARAMS,
        MS,
        RETENTION
    }

    /** 한 벡터(또는 파일 최상위)를 검증하는 문맥. 오류 메시지에 파일명과 위치를 붙인다. */
    private static final class Check {
        private final String fileName;
        private final String where;

        Check(String fileName, String where) {
            this.fileName = fileName;
            this.where = where;
        }

        InvalidVectorException fail(String path, String message) {
            String place = where.isEmpty() ? "" : where + " ";
            String field = path.isEmpty() ? "" : path + ": ";
            return new InvalidVectorException(fileName + ": " + place + field + message);
        }

        void checkFormat(JsonElement value) {
            if (!isInteger(value)) {
                throw fail("format", "정수여야 한다");
            }
            BigInteger format = integer(value);
            if (!format.equals(BigInteger.valueOf(KNOWN_FORMAT))) {
                throw fail("format", "지원하지 않는 format이다(아는 것은 " + KNOWN_FORMAT + "): " + format);
            }
        }

        /** 모르는 필드와 필수 필드 누락을 거부하고, 알려진 필드의 값 종류를 검증한다. */
        JsonObject object(String path, JsonElement element, Map<String, Kind> required, Map<String, Kind> optional) {
            if (!element.isJsonObject()) {
                throw fail(path, "객체여야 한다");
            }
            JsonObject object = element.getAsJsonObject();
            String prefix = path.isEmpty() ? "" : path + ".";
            for (String name : object.keySet()) {
                if (!required.containsKey(name) && !optional.containsKey(name)) {
                    throw fail(path, "알 수 없는 필드: " + name);
                }
            }
            for (String name : required.keySet()) {
                if (!object.has(name)) {
                    throw fail(path, "필수 필드 누락: " + name);
                }
            }
            for (String name : object.keySet()) {
                Kind kind = required.containsKey(name) ? required.get(name) : optional.get(name);
                value(prefix + name, object.get(name), kind);
            }
            return object;
        }

        private void value(String path, JsonElement element, Kind kind) {
            switch (kind) {
                case ANY:
                    return;
                case HEX:
                    hex(path, element);
                    return;
                case U16:
                    range(path, element, 0, U16_MAX);
                    return;
                case U32:
                case STREAM_ID:
                    range(path, element, 0, U32_MAX);
                    return;
                case APP_TYPE:
                    range(path, element, 256, U16_MAX);
                    return;
                case MS:
                case RETENTION:
                    range(path, element, 1, U32_MAX);
                    return;
                case STATUS:
                    named(path, element, STATUS_NAMES, 17, 255);
                    return;
                case ERROR_CODE:
                    named(path, element, ERROR_CODE_NAMES, 9, U32_MAX);
                    return;
                case EXTENSIONS:
                    extensions(path, element);
                    return;
                case HELLO_PARAMS:
                    object(
                            path,
                            element,
                            fields(),
                            fields(
                                    "max_frame_size", Kind.U32,
                                    "max_pending_bytes", Kind.U32,
                                    "max_concurrent_streams", Kind.U32,
                                    "capabilities", Kind.U32,
                                    "content_types", Kind.U32,
                                    "content_type", Kind.U32));
                    return;
                default:
                    throw new AssertionError(kind);
            }
        }

        String string(String path, JsonElement element) {
            if (!isString(element)) {
                throw fail(path, "문자열이어야 한다");
            }
            return element.getAsString();
        }

        JsonArray array(String path, JsonElement element) {
            if (!element.isJsonArray()) {
                throw fail(path, "배열이어야 한다");
            }
            return element.getAsJsonArray();
        }

        private boolean isInteger(JsonElement element) {
            if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isNumber()) {
                return false;
            }
            try {
                element.getAsBigDecimal().toBigIntegerExact();
                return true;
            } catch (ArithmeticException e) {
                return false;
            }
        }

        private BigInteger integer(JsonElement element) {
            return element.getAsBigDecimal().toBigIntegerExact();
        }

        private void range(String path, JsonElement element, long min, long max) {
            if (!isInteger(element)) {
                throw fail(path, "정수여야 한다");
            }
            BigInteger value = integer(element);
            if (value.compareTo(BigInteger.valueOf(min)) < 0 || value.compareTo(BigInteger.valueOf(max)) > 0) {
                throw fail(path, min + " 이상 " + max + " 이하여야 한다: " + value);
            }
        }

        /** 2.5절의 이름이거나, 예약 값 범위의 정수. */
        private void named(String path, JsonElement element, List<String> names, long reservedMin, long reservedMax) {
            if (isString(element)) {
                if (!names.contains(element.getAsString())) {
                    throw fail(path, "알 수 없는 이름: " + element.getAsString());
                }
                return;
            }
            range(path, element, reservedMin, reservedMax);
        }

        /** 소문자 hex. 공백(스페이스·탭·줄바꿈)은 허용하고, 모두 지운 뒤 자릿수가 짝수여야 한다. */
        private void hex(String path, JsonElement element) {
            String text = string(path, element);
            int digits = 0;
            for (char c : text.toCharArray()) {
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    continue;
                }
                if (c >= 'A' && c <= 'F') {
                    throw fail(path, "hex에 대문자가 있다(소문자만 허용): " + c);
                }
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                    throw fail(path, "hex가 아닌 문자: " + c);
                }
                digits++;
            }
            if (digits % 2 != 0) {
                throw fail(path, "hex 길이가 홀수다: " + digits);
            }
        }

        private void extensions(String path, JsonElement element) {
            JsonArray array = array(path, element);
            for (int i = 0; i < array.size(); i++) {
                object(path + "[" + i + "]", array.get(i), fields("tag", Kind.U16, "value", Kind.HEX), fields());
            }
        }

        JsonObject setup(JsonElement element) {
            JsonObject setup = object(
                    "setup",
                    element,
                    fields("role", Kind.ANY, "established", Kind.ANY),
                    fields(
                            "local_hello", Kind.HELLO_PARAMS,
                            "peer_hello", Kind.HELLO_PARAMS,
                            "config", Kind.ANY));
            String role = string("setup.role", setup.get("role"));
            if (!role.equals("client") && !role.equals("server")) {
                throw fail("setup.role", "client 또는 server여야 한다: " + role);
            }
            if (!setup.get("established").isJsonPrimitive()
                    || !setup.get("established").getAsJsonPrimitive().isBoolean()) {
                throw fail("setup.established", "불리언이어야 한다");
            }
            if (setup.has("config")) {
                object(
                        "setup.config",
                        setup.get("config"),
                        fields(),
                        fields("draining_retention_limit", Kind.RETENTION, "hello_timeout_ms", Kind.RETENTION));
            }
            return setup;
        }

        JsonObject step(String path, JsonElement element) {
            if (!element.isJsonObject()) {
                throw fail(path, "객체여야 한다");
            }
            JsonElement kindElement = element.getAsJsonObject().get("kind");
            if (kindElement == null) {
                throw fail(path, "필수 필드 누락: kind");
            }
            String kind = string(path + ".kind", kindElement);
            Map<String, Kind> required = fields("kind", Kind.ANY, "send", Kind.ANY, "events", Kind.ANY);
            Map<String, Kind> optional = fields();
            switch (kind) {
                case "recv":
                    required.put("hex", Kind.HEX);
                    break;
                case "request":
                    required.put("type", Kind.APP_TYPE);
                    optional.put("extensions", Kind.EXTENSIONS);
                    optional.put("payload", Kind.HEX);
                    break;
                case "respond":
                    required.put("stream_id", Kind.STREAM_ID);
                    required.put("status", Kind.STATUS);
                    optional.put("extensions", Kind.EXTENSIONS);
                    optional.put("payload", Kind.HEX);
                    break;
                case "cancel":
                    required.put("stream_id", Kind.STREAM_ID);
                    break;
                case "close":
                    required.put("error_code", Kind.ERROR_CODE);
                    break;
                case "advance":
                    required.put("ms", Kind.MS);
                    break;
                default:
                    throw fail(path + ".kind", "알 수 없는 단계 kind: " + kind);
            }
            JsonObject step = object(path, element, required, optional);
            JsonArray send = array(path + ".send", step.get("send"));
            for (int i = 0; i < send.size(); i++) {
                frame(path + ".send[" + i + "]", send.get(i));
            }
            JsonArray events = array(path + ".events", step.get("events"));
            for (int i = 0; i < events.size(); i++) {
                event(path + ".events[" + i + "]", events.get(i));
            }
            return step;
        }

        private void flags(String path, JsonElement element, List<String> allowed) {
            JsonArray array = array(path, element);
            Set<String> seen = new HashSet<>();
            for (int i = 0; i < array.size(); i++) {
                String flag = string(path + "[" + i + "]", array.get(i));
                if (!allowed.contains(flag)) {
                    throw fail(path + "[" + i + "]", "허용되지 않는 플래그: " + flag + " (허용: " + allowed + ")");
                }
                if (!seen.add(flag)) {
                    throw fail(path + "[" + i + "]", "플래그 중복: " + flag);
                }
            }
        }

        private void frame(String path, JsonElement element) {
            if (!element.isJsonObject()) {
                throw fail(path, "객체여야 한다");
            }
            JsonElement typeElement = element.getAsJsonObject().get("type");
            if (typeElement == null) {
                throw fail(path, "필수 필드 누락: type");
            }
            Map<String, Kind> required = fields("type", Kind.ANY);
            Map<String, Kind> optional = fields();
            boolean appFrame = false;
            if (isString(typeElement)) {
                switch (typeElement.getAsString()) {
                    case "PREFACE":
                        break;
                    case "HELLO":
                        optional.put("flags", Kind.ANY);
                        optional.put("stream_id", Kind.ANY);
                        optional.put("params", Kind.HELLO_PARAMS);
                        break;
                    case "PING":
                        required.put("payload", Kind.ANY);
                        optional.put("flags", Kind.ANY);
                        optional.put("stream_id", Kind.ANY);
                        break;
                    case "CLOSE":
                        required.put("last_stream_id", Kind.U32);
                        required.put("error_code", Kind.ERROR_CODE);
                        optional.put("stream_id", Kind.ANY);
                        break;
                    case "RESET":
                        required.put("stream_id", Kind.U32);
                        required.put("error_code", Kind.ERROR_CODE);
                        break;
                    default:
                        throw fail(path + ".type", "알 수 없는 프레임 type: " + typeElement.getAsString());
                }
            } else {
                range(path + ".type", typeElement, 256, U16_MAX);
                appFrame = true;
                required.put("flags", Kind.ANY);
                required.put("stream_id", Kind.STREAM_ID);
                optional.put("status", Kind.STATUS);
                optional.put("extensions", Kind.EXTENSIONS);
                optional.put("payload", Kind.HEX);
            }
            JsonObject frame = object(path, element, required, optional);
            String name = isString(typeElement) ? typeElement.getAsString() : "";
            if (appFrame) {
                flags(path + ".flags", frame.get("flags"), Arrays.asList("RESPONSE", "END"));
            } else if (name.equals("HELLO") || name.equals("PING")) {
                if (frame.has("flags")) {
                    flags(path + ".flags", frame.get("flags"), Arrays.asList("ACK"));
                }
                if (frame.has("stream_id")) {
                    zero(path + ".stream_id", frame.get("stream_id"));
                }
            }
            if (name.equals("PING")) {
                String payload = string(path + ".payload", frame.get("payload"));
                if (!PING_PAYLOAD.matcher(payload).matches()) {
                    throw fail(path + ".payload", "공백 없는 소문자 16자리 hex여야 한다: '" + payload + "'");
                }
            }
            if (name.equals("CLOSE") && frame.has("stream_id")) {
                zero(path + ".stream_id", frame.get("stream_id"));
            }
            if (name.equals("RESET")) {
                range(path + ".stream_id", frame.get("stream_id"), 1, U32_MAX);
            }
        }

        private void zero(String path, JsonElement element) {
            range(path, element, 0, 0);
        }

        private void event(String path, JsonElement element) {
            if (!element.isJsonObject()) {
                throw fail(path, "객체여야 한다");
            }
            JsonElement nameElement = element.getAsJsonObject().get("event");
            if (nameElement == null) {
                throw fail(path, "필수 필드 누락: event");
            }
            String name = string(path + ".event", nameElement);
            Map<String, Kind> required = fields("event", Kind.ANY);
            switch (name) {
                case "request_received":
                    required.put("stream_id", Kind.STREAM_ID);
                    required.put("type", Kind.APP_TYPE);
                    break;
                case "result":
                    required.put("stream_id", Kind.STREAM_ID);
                    required.put("status", Kind.STATUS);
                    break;
                case "closed":
                    break;
                default:
                    throw fail(path + ".event", "알 수 없는 event: " + name);
            }
            object(path, element, required, fields());
        }
    }
}
