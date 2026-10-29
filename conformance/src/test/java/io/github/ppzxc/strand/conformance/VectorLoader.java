package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.UncheckedIOException;
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

    static List<Vector> load(Path file) {
        try {
            return parse(file.getFileName().toString(), new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static List<Vector> parse(String fileName, String json) {
        JsonElement root;
        try {
            root = JsonParser.parseString(json);
        } catch (JsonParseException e) {
            throw fail(fileName, "JSON 문법 오류: " + e.getMessage());
        }
        if (!root.isJsonArray()) {
            throw fail(fileName, "최상위 값은 벡터 객체의 배열이어야 한다");
        }
        List<Vector> vectors = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        int index = 0;
        for (JsonElement element : root.getAsJsonArray()) {
            Vector vector = parseVector(fileName, index, element);
            if (!ids.add(vector.id())) {
                throw fail(fileName, "id 중복: " + vector.id());
            }
            vectors.add(vector);
            index++;
        }
        return vectors;
    }

    private static Vector parseVector(String fileName, int index, JsonElement element) {
        String where = "[" + index + "]";
        if (!element.isJsonObject()) {
            throw fail(fileName, where + " 벡터는 객체여야 한다");
        }
        JsonObject object = element.getAsJsonObject();
        for (String name : object.keySet()) {
            if (!REQUIRED.contains(name)) {
                throw fail(fileName, where + " 알 수 없는 필드: " + name);
            }
        }
        for (String name : REQUIRED) {
            if (!object.has(name)) {
                throw fail(fileName, where + " 필수 필드 누락: " + name);
            }
        }
        String id = string(fileName, where, object, ID);
        String description = string(fileName, where, object, DESCRIPTION);
        String result = string(fileName, where, object, RESULT);
        String bytes = string(fileName, where + "(" + id + ")", object, BYTES);
        return new Vector(id, description, parseResult(fileName, where, result), parseHex(fileName, where, bytes));
    }

    private static String string(String fileName, String where, JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw fail(fileName, where + " 필드 " + name + "은 문자열이어야 한다");
        }
        return value.getAsString();
    }

    private static Vector.Result parseResult(String fileName, String where, String result) {
        switch (result) {
            case "valid":
                return Vector.Result.VALID;
            case "invalid":
                return Vector.Result.INVALID;
            default:
                throw fail(fileName, where + " result는 valid 또는 invalid여야 한다: " + result);
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
                throw fail(fileName, where + " hex에 대문자가 있다(소문자만 허용): " + c);
            }
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                throw fail(fileName, where + " hex가 아닌 문자: " + c);
            }
            digits.append(c);
        }
        if (digits.length() % 2 != 0) {
            throw fail(fileName, where + " hex 길이가 홀수다: " + digits.length());
        }
        byte[] bytes = new byte[digits.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(digits.substring(2 * i, 2 * i + 2), 16);
        }
        return bytes;
    }

    private static InvalidVectorException fail(String fileName, String message) {
        return new InvalidVectorException(fileName + ": " + message);
    }
}
