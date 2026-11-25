package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class VectorLoader {

    private VectorLoader() {}

    static List<ConformanceVector> load(Path file) {
        try {
            return parse(file.getFileName().toString(), new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static List<ConformanceVector> parse(String fileName, String json) {
        Check fileCheck = Check.forFile(fileName);
        if (json.trim().isEmpty()) {
            throw fileCheck.fail("파일이 비어 있다");
        }
        JsonObject file = fileCheck.object(Check.ROOT, StrictJson.read(fileName, json), VectorSchema.FILE);

        List<ConformanceVector> result = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        JsonArray vectors = file.getAsJsonArray("vectors");
        for (int index = 0; index < vectors.size(); index++) {
            ConformanceVector vector = parseVector(fileName, index, vectors.get(index));
            if (!ids.add(vector.id())) {
                throw Check.forVector(fileName, index, vector.id()).fail("id 중복");
            }
            result.add(vector);
        }
        return result;
    }

    private static ConformanceVector parseVector(String fileName, int index, JsonElement element) {
        Check check = Check.forVector(fileName, index, element);
        JsonObject vector = check.object(Check.ROOT, element, VectorSchema.VECTOR);
        VectorSchema.crossCheck(check, vector);

        List<String> requirements = new ArrayList<>();
        for (JsonElement id : vector.getAsJsonArray("requirements")) {
            requirements.add(id.getAsString());
        }
        List<JsonObject> steps = new ArrayList<>();
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            steps.add(step.getAsJsonObject());
        }
        return new ConformanceVector(
                vector.get("id").getAsString(),
                vector.get("description").getAsString(),
                requirements,
                vector.getAsJsonObject("setup"),
                steps);
    }
}
