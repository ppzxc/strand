package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 로더를 거치지 않고 벡터 파일을 JSON 그대로 읽는 테스트 도구. 읽은 결과는 한 번만 만든다. */
final class RawVectors {

    private static Map<String, JsonObject> byKey;

    private RawVectors() {}

    /** `파일명/id`에서 벡터 객체로. 파일 순서와 파일 안 순서를 지킨다. */
    static synchronized Map<String, JsonObject> byKey() {
        if (byKey == null) {
            Map<String, JsonObject> result = new LinkedHashMap<>();
            for (Path file : files()) {
                for (JsonObject vector : vectors(file)) {
                    result.put(
                            VectorSuite.fileKey(file) + "/" + vector.get("id").getAsString(), vector);
                }
            }
            byKey = Collections.unmodifiableMap(result);
        }
        return byKey;
    }

    static List<Path> files() {
        return VectorSuite.jsonFiles(VectorSuite.defaultDirectory());
    }

    static List<JsonObject> vectors(Path file) {
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            List<JsonObject> result = new ArrayList<>();
            for (JsonElement v :
                    JsonParser.parseReader(reader).getAsJsonObject().getAsJsonArray("vectors")) {
                result.add(v.getAsJsonObject());
            }
            return result;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
