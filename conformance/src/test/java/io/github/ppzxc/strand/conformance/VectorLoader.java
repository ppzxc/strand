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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 적합성 벡터 파일의 엄격 로더. Gson 트리 API로 읽고, 형식을 조금이라도 어기면 {@link InvalidVectorException}을 던진다.
 *
 * <p>파일은 벡터 객체의 JSON 배열이다. 공통 필드는 {@code id}, {@code description}, {@code result}이고
 * {@code bytes}는 규격이 필드를 정하기 전까지 로더·러너를 검증하기 위한 자리표시 필드다. 알 수 없는 필드는 거부한다.
 */
final class VectorLoader {

    private static final String ID = "id";
    private static final String DESCRIPTION = "description";
    private static final String RESULT = "result";
    private static final String BYTES = "bytes";
    private static final List<String> REQUIRED = Arrays.asList(ID, DESCRIPTION, RESULT, BYTES);

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
            throw fail(fileName, "", "파일이 비어 있다");
        }
        JsonElement root = readTree(fileName, json);
        if (!root.isJsonArray()) {
            throw fail(fileName, "", "최상위 값은 벡터 객체의 배열이어야 한다");
        }
        List<ConformanceVector> vectors = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        int index = 0;
        for (JsonElement element : root.getAsJsonArray()) {
            ConformanceVector vector = parseVector(fileName, index, element);
            if (!ids.add(vector.id())) {
                throw fail(fileName, location(index, vector.id()), "id 중복");
            }
            vectors.add(vector);
            index++;
        }
        return vectors;
    }

    /**
     * 엄격 모드 JsonReader로 트리를 만든다. Gson 트리 API는 한 객체 안의 같은 키를 조용히 덮어쓰므로
     * 객체를 직접 조립하면서 중복 키를 거부한다.
     */
    private static JsonElement readTree(String fileName, String json) {
        try (JsonReader reader = new JsonReader(new StringReader(json))) {
            reader.setStrictness(Strictness.STRICT);
            JsonElement root = readValue(fileName, reader);
            if (reader.peek() != JsonToken.END_DOCUMENT) {
                throw fail(fileName, "", "JSON 문법 오류: 최상위 값 뒤에 내용이 더 있다");
            }
            return root;
        } catch (IOException | JsonParseException | IllegalStateException | NumberFormatException e) {
            throw fail(fileName, "", "JSON 문법 오류: " + e.getMessage());
        }
    }

    private static JsonElement readValue(String fileName, JsonReader reader) throws IOException {
        switch (reader.peek()) {
            case BEGIN_OBJECT:
                JsonObject object = new JsonObject();
                reader.beginObject();
                while (reader.hasNext()) {
                    String name = reader.nextName();
                    if (object.has(name)) {
                        throw fail(fileName, "", "JSON 중복 키: " + name);
                    }
                    object.add(name, readValue(fileName, reader));
                }
                reader.endObject();
                return object;
            case BEGIN_ARRAY:
                JsonArray array = new JsonArray();
                reader.beginArray();
                while (reader.hasNext()) {
                    array.add(readValue(fileName, reader));
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
        if (!element.isJsonObject()) {
            throw fail(fileName, location(index, ""), "벡터는 객체여야 한다");
        }
        JsonObject object = element.getAsJsonObject();
        // id를 알 수 있으면 이후 모든 오류 메시지에 id를 넣는다.
        String knownId =
                object.has(ID) && isString(object.get(ID)) ? object.get(ID).getAsString() : "";
        String where = location(index, knownId);
        for (String name : object.keySet()) {
            if (!REQUIRED.contains(name)) {
                throw fail(fileName, where, "알 수 없는 필드: " + name);
            }
        }
        for (String name : REQUIRED) {
            if (!object.has(name)) {
                throw fail(fileName, where, "필수 필드 누락: " + name);
            }
        }
        String id = string(fileName, where, object, ID);
        if (id.isEmpty()) {
            throw fail(fileName, where, "id는 비어 있으면 안 된다");
        }
        String description = string(fileName, where, object, DESCRIPTION);
        String result = string(fileName, where, object, RESULT);
        String bytes = string(fileName, where, object, BYTES);
        return new ConformanceVector(
                id, description, parseResult(fileName, where, result), parseHex(fileName, where, bytes));
    }

    private static boolean isString(JsonElement value) {
        return value.isJsonPrimitive() && value.getAsJsonPrimitive().isString();
    }

    private static String string(String fileName, String where, JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (!isString(value)) {
            throw fail(fileName, where, "필드 " + name + "은 문자열이어야 한다");
        }
        return value.getAsString();
    }

    private static ConformanceVector.Result parseResult(String fileName, String where, String result) {
        switch (result) {
            case "valid":
                return ConformanceVector.Result.VALID;
            case "invalid":
                return ConformanceVector.Result.INVALID;
            default:
                throw fail(fileName, where, "result는 valid 또는 invalid여야 한다: " + result);
        }
    }

    /** 소문자 hex. 공백(스페이스·탭·줄바꿈)은 무시한다. */
    private static byte[] parseHex(String fileName, String where, String text) {
        StringBuilder digits = new StringBuilder();
        for (char c : text.toCharArray()) {
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                continue;
            }
            if (c >= 'A' && c <= 'F') {
                throw fail(fileName, where, "hex에 대문자가 있다(소문자만 허용): " + c);
            }
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                throw fail(fileName, where, "hex가 아닌 문자: " + c);
            }
            digits.append(c);
        }
        if (digits.length() % 2 != 0) {
            throw fail(fileName, where, "hex 길이가 홀수다: " + digits.length());
        }
        byte[] bytes = new byte[digits.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(digits.substring(2 * i, 2 * i + 2), 16);
        }
        return bytes;
    }

    /** 모든 오류는 {@code 파일명: [위치](id=...) 메시지} 형식이다. */
    private static InvalidVectorException fail(String fileName, String where, String message) {
        return new InvalidVectorException(fileName + ": " + (where.isEmpty() ? "" : where + " ") + message);
    }
}
