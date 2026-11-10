package io.github.ppzxc.strand.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.math.BigInteger;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** 필드 규칙의 내부 불변식: 형태 표가 규칙을 직접 들고, 규칙은 경로가 붙은 같은 형식의 오류를 낸다. */
class FieldCheckTest {

    private static final Check CHECK = Check.forFile("f.json");

    @Test
    void rangeRejectsOutsideBoundsAtThePath() {
        FieldCheck check = FieldCheck.range(1, 5);
        assertThatCode(() -> check.check(CHECK, "a.b", JsonParser.parseString("5")))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> check.check(CHECK, "a.b", JsonParser.parseString("6")))
                .hasMessage("f.json: a.b: 1 이상 5 이하여야 한다: 6");
    }

    @Test
    void decimalTakesStringsOnly() {
        FieldCheck check = FieldCheck.decimal(BigInteger.ZERO, BigInteger.ZERO);
        assertThatCode(() -> check.check(CHECK, "id", JsonParser.parseString("\"0\"")))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> check.check(CHECK, "id", JsonParser.parseString("0")))
                .isInstanceOf(InvalidVectorException.class);
    }

    @Test
    void namedAcceptsListedNamesAndReservedIntegersAfterThem() {
        FieldCheck check = FieldCheck.named(Arrays.asList("A", "B"), 5);
        assertThatCode(() -> check.check(CHECK, "s", JsonParser.parseString("\"B\"")))
                .doesNotThrowAnyException();
        assertThatCode(() -> check.check(CHECK, "s", JsonParser.parseString("5")))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> check.check(CHECK, "s", JsonParser.parseString("1")))
                .hasMessage("f.json: s: 2 이상 5 이하여야 한다: 1");
        assertThatThrownBy(() -> check.check(CHECK, "s", JsonParser.parseString("\"C\"")))
                .hasMessage("f.json: s: 알 수 없는 이름: C");
    }

    @Test
    void oneOfAndFlagsRejectValuesOutsideTheAllowedList() {
        assertThatCode(() -> FieldCheck.oneOf(Arrays.asList("x")).check(CHECK, "o", JsonParser.parseString("\"x\"")))
                .doesNotThrowAnyException();
        assertThatThrownBy(
                        () -> FieldCheck.oneOf(Arrays.asList("x")).check(CHECK, "o", JsonParser.parseString("\"y\"")))
                .isInstanceOf(InvalidVectorException.class);
        assertThatCode(() ->
                        FieldCheck.flags(Arrays.asList("ACK")).check(CHECK, "f", JsonParser.parseString("[\"ACK\"]")))
                .doesNotThrowAnyException();
        assertThatThrownBy(() ->
                        FieldCheck.flags(Arrays.asList("ACK")).check(CHECK, "f", JsonParser.parseString("[\"NAK\"]")))
                .isInstanceOf(InvalidVectorException.class);
    }

    @Test
    void deferredNeverInspectsTheValue() {
        assertThatCode(() -> FieldCheck.DEFERRED.check(CHECK, "x", JsonParser.parseString("[]")))
                .doesNotThrowAnyException();
    }

    @Test
    void shapeRunsTheCheckItHoldsForEachDeclaredField() {
        Shape shape = Shape.EMPTY.require("n", FieldCheck.range(1, 5)).allow("o", FieldCheck.oneOf(Arrays.asList("x")));
        JsonObject ok = JsonParser.parseString("{\"n\":2,\"o\":\"x\"}").getAsJsonObject();
        assertThat(CHECK.object("p", ok, shape)).isSameAs(ok);
        assertThatThrownBy(() -> CHECK.object("p", JsonParser.parseString("{\"n\":9}"), shape))
                .hasMessage("f.json: p.n: 1 이상 5 이하여야 한다: 9");
        assertThatThrownBy(() -> shape.checkOf("missing")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nestedObjectCheckReportsTheNestedPath() {
        Shape inner = Shape.EMPTY.require("k", FieldCheck.range(0, 1));
        Shape outer = Shape.EMPTY.require("in", FieldCheck.object(inner));
        assertThatThrownBy(() -> CHECK.object("p", JsonParser.parseString("{\"in\":{\"k\":2}}"), outer))
                .hasMessage("f.json: p.in.k: 0 이상 1 이하여야 한다: 2");
    }
}
