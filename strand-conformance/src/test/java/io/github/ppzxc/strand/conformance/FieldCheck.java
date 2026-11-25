package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonElement;
import java.math.BigInteger;
import java.util.List;

@FunctionalInterface
interface FieldCheck {

    FieldCheck DEFERRED = (c, path, element) -> {};

    void check(Check c, String path, JsonElement element);

    static FieldCheck range(long min, long max) {
        return (c, path, element) -> c.range(path, element, min, max);
    }

    static FieldCheck decimal(BigInteger min, BigInteger max) {
        return (c, path, element) -> c.decimal(path, element, min, max);
    }

    static FieldCheck named(List<String> names, long reservedMax) {
        return (c, path, element) -> c.named(path, element, names, reservedMax);
    }

    static FieldCheck oneOf(List<String> allowed) {
        return (c, path, element) -> c.oneOf(path, element, allowed);
    }

    static FieldCheck flags(List<String> allowed) {
        return (c, path, element) -> c.flags(path, element, allowed);
    }

    static FieldCheck object(Shape shape) {
        return (c, path, element) -> c.object(path, element, shape);
    }
}
