package io.github.ppzxc.strand.conformance;

import com.google.gson.JsonElement;
import java.math.BigInteger;
import java.util.List;

/** 필드 값 하나를 검증하는 규칙. 형태 표({@link Shape})가 필드마다 하나씩 직접 가진다. */
@FunctionalInterface
interface FieldCheck {

    /** 객체 검증이 값을 보지 않는다. 호출한 쪽이 객체 검증 앞이나 뒤에 따로 검증한다. */
    FieldCheck DEFERRED = (c, path, element) -> {};

    void check(Check c, String path, JsonElement element);

    /** min 이상 max 이하의 정수. */
    static FieldCheck range(long min, long max) {
        return (c, path, element) -> c.range(path, element, min, max);
    }

    /** min 이상 max 이하의 10진수 문자열. */
    static FieldCheck decimal(BigInteger min, BigInteger max) {
        return (c, path, element) -> c.decimal(path, element, min, max);
    }

    /** 이름 목록 중 하나이거나, 이름 목록 다음부터 reservedMax까지의 예약 정수. */
    static FieldCheck named(List<String> names, long reservedMax) {
        return (c, path, element) -> c.named(path, element, names, reservedMax);
    }

    static FieldCheck oneOf(List<String> allowed) {
        return (c, path, element) -> c.oneOf(path, element, allowed);
    }

    static FieldCheck flags(List<String> allowed) {
        return (c, path, element) -> c.flags(path, element, allowed);
    }

    /** 중첩 객체. 값은 이 형태를 따라야 한다. */
    static FieldCheck object(Shape shape) {
        return (c, path, element) -> c.object(path, element, shape);
    }
}
