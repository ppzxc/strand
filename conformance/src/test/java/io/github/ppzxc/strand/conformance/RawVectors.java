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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 로더를 거치지 않고 벡터 파일을 JSON 그대로 읽는 테스트 도구. 파일 목록, 파일별 벡터, `byKey()`의 결과를 벡터 디렉터리별로 한 번만
 * 만들어 둔다. 디렉터리가 바뀌면 처음부터 다시 읽는다.
 */
final class RawVectors {

    private static Path cachedDirectory;
    private static List<Path> files;
    private static Map<Path, List<JsonObject>> vectorsByFile;
    private static Map<String, JsonObject> byKey;

    private RawVectors() {}

    /** `파일명/id`에서 벡터 객체로. 파일 순서와 파일 안 순서를 지킨다. */
    static synchronized Map<String, JsonObject> byKey() {
        refreshIfDirectoryChanged();
        if (byKey == null) {
            Map<String, JsonObject> result = new LinkedHashMap<>();
            for (Path file : files()) {
                for (JsonObject vector : vectors(file)) {
                    put(
                            result,
                            VectorSuite.fileKey(file) + "/" + vector.get("id").getAsString(),
                            vector);
                }
            }
            byKey = Collections.unmodifiableMap(result);
        }
        return byKey;
    }

    /** 같은 키가 두 번 들어오면 앞의 벡터가 가려지므로 거부한다. */
    static void put(Map<String, JsonObject> target, String key, JsonObject vector) {
        if (target.put(key, vector) != null) {
            throw new IllegalStateException("벡터 키가 겹친다: " + key);
        }
    }

    static synchronized List<Path> files() {
        refreshIfDirectoryChanged();
        if (files == null) {
            files = Collections.unmodifiableList(VectorSuite.jsonFiles(cachedDirectory));
        }
        return files;
    }

    static synchronized List<JsonObject> vectors(Path file) {
        refreshIfDirectoryChanged();
        return vectorsByFile.computeIfAbsent(file, RawVectors::read);
    }

    private static void refreshIfDirectoryChanged() {
        Path directory = VectorSuite.defaultDirectory();
        if (!directory.equals(cachedDirectory)) {
            cachedDirectory = directory;
            files = null;
            vectorsByFile = new HashMap<>();
            byKey = null;
        }
    }

    private static List<JsonObject> read(Path file) {
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            List<JsonObject> result = new ArrayList<>();
            for (JsonElement v :
                    JsonParser.parseReader(reader).getAsJsonObject().getAsJsonArray("vectors")) {
                result.add(v.getAsJsonObject());
            }
            return Collections.unmodifiableList(result);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
