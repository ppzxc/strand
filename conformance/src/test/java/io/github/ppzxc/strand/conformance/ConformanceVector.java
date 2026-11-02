package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

/** 적합성 벡터 하나(벡터 형식 문서 3절). 로더가 벡터 형식 문서 규칙을 검증한 뒤에만 만들어진다. */
final class ConformanceVector {

    private final String id;
    private final String description;
    private final List<String> spec;
    private final JsonObject setup;
    private final List<JsonObject> steps;

    ConformanceVector(String id, String description, List<String> spec, JsonObject setup, List<JsonObject> steps) {
        this.id = id;
        this.description = description;
        this.spec = frozen(spec, UnaryOperator.<String>identity());
        this.setup = setup.deepCopy();
        this.steps = frozen(steps, JsonObject::deepCopy);
    }

    /** 원소를 복사해 담은, 수정할 수 없는 목록. 호출자가 넘긴 목록이나 원소를 나중에 바꿔도 영향받지 않는다. */
    private static <T> List<T> frozen(List<T> items, UnaryOperator<T> copier) {
        List<T> copy = new ArrayList<>(items.size());
        for (T item : items) {
            copy.add(copier.apply(item));
        }
        return Collections.unmodifiableList(copy);
    }

    String id() {
        return id;
    }

    String description() {
        return description;
    }

    /** 근거가 되는 규격 절 번호. 비어 있지 않다. */
    List<String> spec() {
        return spec;
    }

    /** 검증을 마친 초기 상태(벡터 형식 문서 4절). 생략된 선택 필드는 없는 그대로다. */
    JsonObject setup() {
        return setup.deepCopy();
    }

    /** 검증을 마친 단계 목록(벡터 형식 문서 5절). 하나 이상이다. */
    List<JsonObject> steps() {
        return steps;
    }
}
