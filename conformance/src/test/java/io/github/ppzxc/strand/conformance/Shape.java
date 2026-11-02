package io.github.ppzxc.strand.conformance;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * JSON 객체의 형태: 필수 필드와 선택 필드, 각 필드의 값 종류. 불변이며 {@code require}/{@code allow}는 새 형태를 돌려준다.
 */
final class Shape {

    static final Shape EMPTY =
            new Shape(Collections.<String, FieldKind>emptyMap(), Collections.<String, FieldKind>emptyMap());

    private final Map<String, FieldKind> required;
    private final Map<String, FieldKind> optional;

    private Shape(Map<String, FieldKind> required, Map<String, FieldKind> optional) {
        this.required = required;
        this.optional = optional;
    }

    Shape require(String name, FieldKind kind) {
        Map<String, FieldKind> next = new LinkedHashMap<>(required);
        next.put(name, kind);
        return new Shape(next, optional);
    }

    Shape allow(String name, FieldKind kind) {
        Map<String, FieldKind> next = new LinkedHashMap<>(optional);
        next.put(name, kind);
        return new Shape(required, next);
    }

    boolean declares(String name) {
        return required.containsKey(name) || optional.containsKey(name);
    }

    /** 선언된 필드의 값 종류. */
    FieldKind kindOf(String name) {
        FieldKind kind = required.containsKey(name) ? required.get(name) : optional.get(name);
        if (kind == null) {
            throw new IllegalArgumentException("선언되지 않은 필드: " + name);
        }
        return kind;
    }

    Set<String> requiredNames() {
        return Collections.unmodifiableSet(required.keySet());
    }
}
