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

/**
 * 파일 하나 또는 그 안의 벡터 하나를 검증하는 문맥. 값 종류별 검증 규칙을 모아 두고, 오류 메시지 형식
 * ({@code 파일명: [위치](id=...) 경로: 메시지})을 한 곳에서 조립한다.
 */
final class Check {

    /** 검증 대상 객체 자체를 가리키는 경로. 오류 메시지에서는 경로 부분이 생략된다. */
    static final String ROOT = "";

    private static final Pattern ID = Pattern.compile("[a-z0-9]+(-[a-z0-9]+)*");
    private static final Pattern PING_PAYLOAD = Pattern.compile("[0-9a-f]{16}");
    private static final Pattern SPEC_SECTION = Pattern.compile("(?:[0-9]+|[A-G])(?:\\.[0-9]+)*");

    private static final Pattern DECIMAL = Pattern.compile("0|[1-9][0-9]*");

    private final String fileName;
    private final String location;

    private Check(String fileName, String location) {
        this.fileName = fileName;
        this.location = location;
    }

    /** 파일 전체(최상위 객체)를 검증하는 문맥. 오류 메시지에 위치가 붙지 않는다. */
    static Check forFile(String fileName) {
        return new Check(fileName, "");
    }

    /** {@code vectors} 배열의 index번째 원소를 검증하는 문맥. id를 알면 이후 모든 오류 메시지에 넣는다. */
    static Check forVector(String fileName, int index, JsonElement element) {
        return new Check(fileName, locationOf(index, idOrEmpty(element)));
    }

    /** 같은 파일의 다른 벡터에 대한 오류(예: id 중복)를 만들 문맥. */
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

    /** 문맥 전체에 대한 오류. 특정 필드를 가리키지 않는다. */
    InvalidVectorException fail(String message) {
        return new InvalidVectorException(fileName + ": " + (location.isEmpty() ? "" : location + " ") + message);
    }

    /** 경로가 가리키는 필드에 대한 오류. */
    InvalidVectorException failAt(String path, String message) {
        return path.equals(ROOT) ? fail(message) : fail(path + ": " + message);
    }

    static String child(String path, String name) {
        return path.equals(ROOT) ? name : path + "." + name;
    }

    static String item(String path, int index) {
        return path + "[" + index + "]";
    }

    // ---- 구조 ----

    /** 객체인지 확인하고, 모르는 필드와 필수 필드 누락을 거부한 뒤, 알려진 필드의 값 종류를 검증한다. */
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
            shape.kindOf(name).check(this, child(path, name), object.get(name));
        }
        return object;
    }

    /**
     * 판별 필드로 형태가 정해지는 객체(단계·프레임·이벤트)의 공통 앞부분: 객체인지 확인하고 판별 필드 값을 꺼낸다.
     * 판별 필드가 없으면 "필수 필드 누락: 필드명"으로 거부한다.
     */
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

    /** 판별 필드가 문자열 이름인 객체를 검증한다. 이름에 해당하는 형태를 표에서 찾고, 모르는 이름이면 거부한다. */
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

    // ---- 벡터 최상위 필드 ----

    void format(String path, JsonElement value, int knownFormat) {
        if (!isInteger(value)) {
            throw failAt(path, "정수여야 한다");
        }
        BigInteger format = integer(value);
        if (!format.equals(BigInteger.valueOf(knownFormat))) {
            throw failAt(path, "지원하지 않는 format이다(아는 것은 " + knownFormat + "): " + format);
        }
    }

    /** 벡터 id: kebab-case여야 한다. 파일 안의 고유성은 파일 단위라 로더가 본다. */
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

    /** 근거 규격 절 번호 목록. 비어 있지 않고 모두 절 번호 형식이어야 한다. */
    void specSections(String path, JsonElement element) {
        JsonArray array = array(path, element);
        if (array.isEmpty()) {
            throw failAt(path, "비어 있으면 안 된다");
        }
        for (int i = 0; i < array.size(); i++) {
            String section = string(item(path, i), array.get(i));
            if (!SPEC_SECTION.matcher(section).matches()) {
                throw failAt(item(path, i), "규격 절 번호가 아니다: '" + section + "'");
            }
        }
    }

    void oneOf(String path, JsonElement element, List<String> allowed) {
        String value = string(path, element);
        if (!allowed.contains(value)) {
            throw failAt(path, String.join(" 또는 ", allowed) + "여야 한다: " + value);
        }
    }

    /** PING payload: 공백 없는 소문자 16자리 hex(8바이트). */
    void pingPayload(String path, JsonElement element) {
        String payload = string(path, element);
        if (!PING_PAYLOAD.matcher(payload).matches()) {
            throw failAt(path, "공백 없는 소문자 16자리 hex여야 한다: '" + payload + "'");
        }
    }

    // ---- 값 ----

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

    /** 10진수 문자열로 쓴 정수. 부호와 앞자리 0은 허용하지 않고, {@code min} 이상 {@code max} 이하여야 한다. */
    void decimal(String path, JsonElement element, BigInteger min, BigInteger max) {
        if (!isString(element)) {
            throw failAt(path, "10진수 문자열이어야 한다(JSON 수가 아니다)");
        }
        String text = element.getAsString();
        if (text.length() > max.toString().length() || !DECIMAL.matcher(text).matches()) {
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

    /** 이름 목록에 있는 이름이거나, 목록 다음 값부터 {@code reservedMax}까지의 예약 정수. 목록 순서가 곧 코드 값이다. */
    void named(String path, JsonElement element, List<String> names, long reservedMax) {
        if (isString(element)) {
            if (!names.contains(element.getAsString())) {
                throw failAt(path, "알 수 없는 이름: " + element.getAsString());
            }
            return;
        }
        range(path, element, names.size(), reservedMax);
    }

    /** 소문자 hex. 공백(스페이스·탭·줄바꿈)은 허용하고, 모두 지운 뒤 자릿수가 짝수여야 한다. */
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

    /** 256 이상 u16 정수의 배열. 같은 값이 두 번 나오면 거부한다. */
    void uniqueAppTypes(String path, JsonElement element) {
        JsonArray array = array(path, element);
        Set<BigInteger> seen = new HashSet<>();
        for (int i = 0; i < array.size(); i++) {
            FieldKind.APP_TYPE.check(this, item(path, i), array.get(i));
            BigInteger value = integer(array.get(i));
            if (!seen.add(value)) {
                throw failAt(item(path, i), "중복 값: " + value);
            }
        }
    }

    /** 객체의 정수 필드 값. 값은 이미 검증됐다고 본다. */
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
