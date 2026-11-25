package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

final class Check {

    static final String ROOT = "";

    private static final int MAX_DECIMAL_DIGITS = 20;

    private static final Pattern ID = Pattern.compile("[a-z0-9]+(-[a-z0-9]+)*");
    private static final Pattern PING_PAYLOAD = Pattern.compile("[0-9a-f]{16}");

    static final Pattern REQUIREMENT_ID = Pattern.compile("req-[a-z0-9]+(-[a-z0-9]+)*");
    private static final Pattern DECIMAL = Pattern.compile("0|[1-9][0-9]*");

    private static final Pattern MECHANISM = Pattern.compile("[A-Z0-9_-]{1,20}");

    private final String fileName;
    private final String location;

    private Check(String fileName, String location) {
        this.fileName = fileName;
        this.location = location;
    }

    static Check forFile(String fileName) {
        return new Check(fileName, "");
    }

    static Check forVector(String fileName, int index, JsonElement element) {
        return new Check(fileName, locationOf(index, idOrEmpty(element)));
    }

    static Check forVector(String fileName, int index, String id) {
        return new Check(fileName, locationOf(index, id));
    }

    private static String locationOf(int index, String id) {
        return id.isEmpty() ? "[" + index + "]" : "[" + index + "](id=" + id + ")";
    }

    private static String idOrEmpty(JsonElement element) {
        if (element.isJsonObject() && isString(element.getAsJsonObject().get("id"))) {
            return element.getAsJsonObject().get("id").getAsString();
        }
        return "";
    }

    InvalidVectorException fail(String message) {
        return new InvalidVectorException(fileName + ": " + (location.isEmpty() ? "" : location + " ") + message);
    }

    InvalidVectorException failAt(String path, String message) {
        return path.equals(ROOT) ? fail(message) : fail(path + ": " + message);
    }

    static String child(String path, String name) {
        return path.equals(ROOT) ? name : path + "." + name;
    }

    static String item(String path, int index) {
        return path + "[" + index + "]";
    }

    JsonObject object(String path, JsonElement element, Shape shape) {
        if (!element.isJsonObject()) {
            throw failAt(path, "객체여야 한다");
        }
        JsonObject object = element.getAsJsonObject();
        for (String name : object.keySet()) {
            if (!shape.declares(name)) {
                throw failAt(path, "알 수 없는 필드: " + name);
            }
        }
        for (String name : shape.requiredNames()) {
            if (!object.has(name)) {
                throw failAt(path, "필수 필드 누락: " + name);
            }
        }
        for (String name : object.keySet()) {
            shape.checkOf(name).check(this, child(path, name), object.get(name));
        }
        return object;
    }

    JsonElement tag(String path, JsonElement element, String tagField) {
        if (!element.isJsonObject()) {
            throw failAt(path, "객체여야 한다");
        }
        JsonElement tag = element.getAsJsonObject().get(tagField);
        if (tag == null) {
            throw failAt(path, "필수 필드 누락: " + tagField);
        }
        return tag;
    }

    JsonObject objectNamedBy(
            String path, JsonElement element, String tagField, String tagLabel, Map<String, Shape> shapesByName) {
        String tagPath = child(path, tagField);
        String name = string(tagPath, tag(path, element, tagField));
        return object(path, element, shapeFor(tagPath, tagLabel, shapesByName, name));
    }

    Shape shapeFor(String tagPath, String tagLabel, Map<String, Shape> shapesByName, String name) {
        Shape shape = shapesByName.get(name);
        if (shape == null) {
            throw failAt(tagPath, "알 수 없는 " + tagLabel + ": " + name);
        }
        return shape;
    }

    String string(String path, JsonElement element) {
        if (!isString(element)) {
            throw failAt(path, "문자열이어야 한다");
        }
        return element.getAsString();
    }

    JsonArray array(String path, JsonElement element) {
        if (!element.isJsonArray()) {
            throw failAt(path, "배열이어야 한다");
        }
        return element.getAsJsonArray();
    }

    void bool(String path, JsonElement element) {
        if (!element.isJsonPrimitive() || !element.getAsJsonPrimitive().isBoolean()) {
            throw failAt(path, "불리언이어야 한다");
        }
    }

    static boolean isString(JsonElement value) {
        return value != null
                && value.isJsonPrimitive()
                && value.getAsJsonPrimitive().isString();
    }

    void format(String path, JsonElement value, int knownFormat) {
        if (!isInteger(value)) {
            throw failAt(path, "정수여야 한다");
        }
        BigInteger format = integer(value);
        if (!format.equals(BigInteger.valueOf(knownFormat))) {
            throw failAt(path, "지원하지 않는 format이다(아는 것은 " + knownFormat + "): " + format);
        }
    }

    void id(String path, JsonElement element) {
        String id = string(path, element);
        if (!ID.matcher(id).matches()) {
            throw failAt(path, "kebab-case([a-z0-9]+(-[a-z0-9]+)*)가 아니다: '" + id + "'");
        }
    }

    void nonEmptyText(String path, JsonElement element) {
        if (string(path, element).isEmpty()) {
            throw failAt(path, "비어 있으면 안 된다");
        }
    }

    void requirementIds(String path, JsonElement element) {
        JsonArray array = array(path, element);
        if (array.isEmpty()) {
            throw failAt(path, "비어 있으면 안 된다");
        }
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < array.size(); i++) {
            String id = string(item(path, i), array.get(i));
            if (!REQUIREMENT_ID.matcher(id).matches()) {
                throw failAt(item(path, i), "요구사항 식별자가 아니다(req-로 시작하는 소문자 kebab-case): '" + id + "'");
            }
            if (!seen.add(id)) {
                throw failAt(item(path, i), "요구사항 식별자가 두 번 나온다: '" + id + "'");
            }
        }
    }

    void oneOf(String path, JsonElement element, List<String> allowed) {
        String value = string(path, element);
        if (!allowed.contains(value)) {
            throw failAt(path, String.join(" 또는 ", allowed) + "여야 한다: " + value);
        }
    }

    void mechanism(String path, JsonElement element) {
        String mechanism = string(path, element);
        if (!MECHANISM.matcher(mechanism).matches()) {
            throw failAt(path, "대문자·숫자·하이픈·밑줄로 이뤄진 1~20자여야 한다: '" + mechanism + "'");
        }
    }

    void pingPayload(String path, JsonElement element) {
        String payload = string(path, element);
        if (!PING_PAYLOAD.matcher(payload).matches()) {
            throw failAt(path, "공백 없는 소문자 16자리 hex여야 한다: '" + payload + "'");
        }
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

    void decimal(String path, JsonElement element, BigInteger min, BigInteger max) {
        if (!isString(element)) {
            throw failAt(path, "10진수 문자열이어야 한다(JSON 수가 아니다)");
        }
        String text = element.getAsString();
        if (text.length() > MAX_DECIMAL_DIGITS || !DECIMAL.matcher(text).matches()) {
            throw failAt(path, "앞자리 0과 부호 없는 10진수 문자열이어야 한다: '" + text + "'");
        }
        BigInteger value = new BigInteger(text);
        if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
            throw failAt(path, min + " 이상 " + max + " 이하여야 한다: " + value);
        }
    }

    void range(String path, JsonElement element, long min, long max) {
        if (!isInteger(element)) {
            throw failAt(path, "정수여야 한다");
        }
        BigInteger value = integer(element);
        if (value.compareTo(BigInteger.valueOf(min)) < 0 || value.compareTo(BigInteger.valueOf(max)) > 0) {
            throw failAt(path, min + " 이상 " + max + " 이하여야 한다: " + value);
        }
    }

    void named(String path, JsonElement element, List<String> names, long reservedMax) {
        if (isString(element)) {
            if (!names.contains(element.getAsString())) {
                throw failAt(path, "알 수 없는 이름: " + element.getAsString());
            }
            return;
        }
        range(path, element, names.size(), reservedMax);
    }

    void hex(String path, JsonElement element) {
        String text = string(path, element);
        int digits = 0;
        for (char c : text.toCharArray()) {
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                continue;
            }
            if (c >= 'A' && c <= 'F') {
                throw failAt(path, "hex에 대문자가 있다(소문자만 허용): " + c);
            }
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                throw failAt(path, "hex가 아닌 문자: " + c);
            }
            digits++;
        }
        if (digits % 2 != 0) {
            throw failAt(path, "hex 길이가 홀수다: " + digits);
        }
    }

    void extensions(String path, JsonElement element) {
        JsonArray array = array(path, element);
        for (int i = 0; i < array.size(); i++) {
            object(item(path, i), array.get(i), VectorSchema.EXTENSION_ENTRY);
        }
    }

    void uniqueAppTypes(String path, JsonElement element) {
        JsonArray array = array(path, element);
        Set<BigInteger> seen = new HashSet<>();
        for (int i = 0; i < array.size(); i++) {
            VectorSchema.APP_TYPE.check(this, item(path, i), array.get(i));
            BigInteger value = integer(array.get(i));
            if (!seen.add(value)) {
                throw failAt(item(path, i), "중복 값: " + value);
            }
        }
    }

    long longOf(JsonObject object, String name) {
        return integer(object.get(name)).longValueExact();
    }

    void flags(String path, JsonElement element, List<String> allowed) {
        JsonArray array = array(path, element);
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < array.size(); i++) {
            String flag = string(item(path, i), array.get(i));
            if (!allowed.contains(flag)) {
                throw failAt(item(path, i), "허용되지 않는 플래그: " + flag + " (허용: " + allowed + ")");
            }
            if (!seen.add(flag)) {
                throw failAt(item(path, i), "플래그 중복: " + flag);
            }
        }
    }
}
