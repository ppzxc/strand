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
import java.math.BigDecimal;

final class StrictJson {

    private StrictJson() {}

    static JsonElement read(String fileName, String json) {
        try (JsonReader reader = new JsonReader(new StringReader(json))) {
            reader.setStrictness(Strictness.STRICT);
            JsonElement root = readValue(fileName, reader);
            if (reader.peek() != JsonToken.END_DOCUMENT) {
                throw fail(fileName, "JSON 문법 오류: 최상위 값 뒤에 내용이 더 있다");
            }
            return root;
        } catch (IOException | JsonParseException | IllegalStateException | NumberFormatException e) {
            throw fail(fileName, "JSON 문법 오류: " + e.getMessage());
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
                        throw fail(fileName, "JSON 중복 키: " + name);
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

    private static InvalidVectorException fail(String fileName, String message) {
        return Check.forFile(fileName).fail(message);
    }
}
