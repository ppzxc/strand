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
 * 만들어 둔다. 디렉터리가 바뀌면 처음부터 다시 읽는다. 돌려주는 JSON 객체는 모든 호출자가 함께 쓰므로 읽기만 해야 한다(고치면 다른
 * 테스트가 보는 값이 바뀐다).
 */
final class RawVectors {

    private static Snapshot snapshot;

    private RawVectors() {}

    /** `파일명/id`에서 벡터 객체로. 파일 순서와 파일 안 순서를 지킨다. */
    static synchronized Map<String, JsonObject> byKey() {
        Snapshot current = current();
        if (current.byKey == null) {
            Map<String, JsonObject> result = new LinkedHashMap<>();
            for (Path file : current.files) {
                for (JsonObject vector : current.vectors(file)) {
                    put(
                            result,
                            VectorSuite.fileKey(file) + "/" + vector.get("id").getAsString(),
                            vector);
                }
            }
            current.byKey = Collections.unmodifiableMap(result);
        }
        return current.byKey;
    }

    /** 같은 키가 두 번 들어오면 앞의 벡터가 가려지므로 거부한다. */
    static void put(Map<String, JsonObject> target, String key, JsonObject vector) {
        if (target.put(key, vector) != null) {
            throw new IllegalStateException("벡터 키가 겹친다: " + key);
        }
    }

    static synchronized List<Path> files() {
        return current().files;
    }

    static synchronized List<JsonObject> vectors(Path file) {
        return current().vectors(file);
    }

    /** 지금 벡터 디렉터리의 읽기 결과. 디렉터리가 바뀌었으면 통째로 새로 만든다. */
    private static Snapshot current() {
        Path directory = VectorSuite.defaultDirectory();
        if (snapshot == null || !snapshot.directory.equals(directory)) {
            snapshot = new Snapshot(directory);
        }
        return snapshot;
    }

    /** 한 벡터 디렉터리의 파일 목록과, 필요할 때 채우는 파일별 벡터와 `byKey()` 결과. */
    private static final class Snapshot {
        final Path directory;
        final List<Path> files;
        final Map<Path, List<JsonObject>> vectorsByFile = new HashMap<>();
        Map<String, JsonObject> byKey;

        Snapshot(Path directory) {
            this.directory = directory;
            this.files = Collections.unmodifiableList(VectorSuite.jsonFiles(directory));
        }

        List<JsonObject> vectors(Path file) {
            return vectorsByFile.computeIfAbsent(file, RawVectors::read);
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
