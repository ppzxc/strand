package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

final class ConformanceVector {

    private final String id;
    private final String description;
    private final List<String> requirements;
    private final JsonObject setup;
    private final List<JsonObject> steps;

    ConformanceVector(
            String id, String description, List<String> requirements, JsonObject setup, List<JsonObject> steps) {
        this.id = id;
        this.description = description;
        this.requirements = frozen(requirements, UnaryOperator.<String>identity());
        this.setup = setup.deepCopy();
        this.steps = frozen(steps, JsonObject::deepCopy);
    }

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

    List<String> requirements() {
        return requirements;
    }

    JsonObject setup() {
        return setup.deepCopy();
    }

    List<JsonObject> steps() {
        return steps;
    }
}
