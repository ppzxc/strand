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
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

final class RawVectors {

    private static Snapshot snapshot;

    private RawVectors() {}

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

    static void put(Map<String, JsonObject> target, String key, JsonObject vector) {
        if (target.put(key, vector) != null) {
            throw new IllegalStateException("벡터 키가 겹친다: " + key);
        }
    }

    static List<String> requirementsOf(JsonObject vector) {
        if (!vector.has("requirements")) {
            throw new IllegalStateException("requirements가 없는 벡터: " + vector.get("id"));
        }
        List<String> ids = new ArrayList<>();
        for (JsonElement id : vector.getAsJsonArray("requirements")) {
            ids.add(id.getAsString());
        }
        return ids;
    }

    static Map<String, List<String>> requirementsByKey() {
        Map<String, List<String>> result = new TreeMap<>();
        for (Map.Entry<String, JsonObject> vector : byKey().entrySet()) {
            result.put(vector.getKey(), requirementsOf(vector.getValue()));
        }
        return result;
    }

    static SortedSet<String> referencedRequirements() {
        return referencedIn(requirementsByKey());
    }

    static SortedSet<String> referencedIn(Map<String, List<String>> byVector) {
        SortedSet<String> result = new TreeSet<>();
        for (List<String> ids : byVector.values()) {
            result.addAll(ids);
        }
        return result;
    }

    static synchronized List<Path> files() {
        return current().files;
    }

    static synchronized List<JsonObject> vectors(Path file) {
        return current().vectors(file);
    }

    private static Snapshot current() {
        Path directory = VectorSuite.defaultDirectory();
        if (snapshot == null || !snapshot.directory.equals(directory)) {
            snapshot = new Snapshot(directory);
        }
        return snapshot;
    }

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
