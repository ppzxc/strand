package io.github.ppzxc.strand.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * core(구동용)와 strand-api(애플리케이션용)는 서로 의존하지 않아 표를 합치지 않는다. 두 표의 값이 같음은 둘을 모두 보는 이 모듈이 고정한다(규격 2.5.1절,
 * 1.3절).
 */
class TableParityTest {

    @Test
    void statusTablesHaveTheSameCodeSetAndValues() {
        assertThat(statusConstants(io.github.ppzxc.strand.core.internal.Status.class))
                .as("core와 api의 Status 코드 이름과 값")
                .isEqualTo(statusConstants(io.github.ppzxc.strand.api.Status.class));
    }

    @Test
    void coreStatusTableEndsAtSixteenLikeTheApi() {
        assertThat(io.github.ppzxc.strand.core.internal.Status.UNAUTHENTICATED)
                .isEqualTo(io.github.ppzxc.strand.api.Status.UNAUTHENTICATED)
                .isEqualTo(16);
        assertThat(io.github.ppzxc.strand.core.internal.Status.OK).isEqualTo(io.github.ppzxc.strand.api.Status.OK);
    }

    @Test
    void certaintyTablesHaveTheSameNamesInTheSameOrder() {
        assertThat(names(io.github.ppzxc.strand.core.internal.Certainty.values()))
                .containsExactly(names(io.github.ppzxc.strand.api.Certainty.values()));
    }

    @Test
    void certaintyTablesExposeTheSameValuesAsTheApi() {
        // 이름·순서에 더해 api가 values()로 노출하는 상수 전체와 서수까지 같아야 한다.
        assertThat(ordinals(io.github.ppzxc.strand.core.internal.Certainty.values()))
                .isEqualTo(ordinals(io.github.ppzxc.strand.api.Certainty.values()));
    }

    private static Map<String, Integer> ordinals(Enum<?>[] values) {
        Map<String, Integer> ordinals = new java.util.LinkedHashMap<>();
        for (Enum<?> value : values) {
            ordinals.put(value.name(), value.ordinal());
        }
        return ordinals;
    }

    private static Map<String, Integer> statusConstants(Class<?> table) {
        Map<String, Integer> constants = new TreeMap<>();
        for (Field field : table.getDeclaredFields()) {
            if (isPublicIntConstant(field)) {
                try {
                    constants.put(field.getName(), field.getInt(null));
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
            }
        }
        return constants;
    }

    private static boolean isPublicIntConstant(Field field) {
        int modifiers = field.getModifiers();
        return Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers) && field.getType() == int.class;
    }

    private static String[] names(Enum<?>[] values) {
        return Arrays.stream(values).map(Enum::name).toArray(String[]::new);
    }
}
