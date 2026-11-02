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

/**
 * 적합성 벡터 파일의 엄격 로더. 파일을 읽어 {@link StrictJson}으로 트리를 만들고, {@link VectorSchema}의 규칙으로 벡터 형식
 * 문서를 검증한 뒤 {@link ConformanceVector}로 조립한다. 하나라도 어기면 {@link InvalidVectorException}을 던진다. JSON
 * Schema 파일({@code conformance/schema/})은 다른 언어 구현자를 위한 보조 자료이고, 어긋나면 벡터 형식 문서가 우선한다.
 *
 * <p>모든 오류는 {@code 파일명: [위치](id=...) 경로: 메시지} 형식이다({@link Check}).
 */
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

    /** 스키마 검증을 통과한 벡터 객체를 {@link ConformanceVector}로 옮긴다. 값은 이미 검증됐다. */
    private static ConformanceVector parseVector(String fileName, int index, JsonElement element) {
        JsonObject vector = Check.forVector(fileName, index, element).object(Check.ROOT, element, VectorSchema.VECTOR);

        List<String> spec = new ArrayList<>();
        for (JsonElement section : vector.getAsJsonArray("spec")) {
            spec.add(section.getAsString());
        }
        List<JsonObject> steps = new ArrayList<>();
        for (JsonElement step : vector.getAsJsonArray("steps")) {
            steps.add(step.getAsJsonObject());
        }
        return new ConformanceVector(
                vector.get("id").getAsString(),
                vector.get("description").getAsString(),
                spec,
                vector.getAsJsonObject("setup"),
                steps);
    }
}
