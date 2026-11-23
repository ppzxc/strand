package io.github.ppzxc.strand.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * core(구동용)와 strand-api(애플리케이션용)는 서로 의존하지 않아 표를 합치지 않는다. 두 표의 값이 같음은 둘을 모두 보는 이 모듈이 고정한다(규격 2.5.1절,
 * 1.3절).
 */
class TableParityTest {

    @Test
    void everyCoreStatusConstantHasTheSameNameAndValueInTheApi() throws Exception {
        for (Field core : io.github.ppzxc.strand.core.internal.Status.class.getDeclaredFields()) {
            if (!isPublicIntConstant(core)) {
                continue;
            }
            Field api = io.github.ppzxc.strand.api.Status.class.getField(core.getName());
            assertThat(api.getInt(null)).as(core.getName()).isEqualTo(core.getInt(null));
        }
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

    private static boolean isPublicIntConstant(Field field) {
        int modifiers = field.getModifiers();
        return Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers) && field.getType() == int.class;
    }

    private static String[] names(Enum<?>[] values) {
        return Arrays.stream(values).map(Enum::name).toArray(String[]::new);
    }
}
