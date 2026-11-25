package io.github.ppzxc.strand.quality;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.tngtech.archunit.core.importer.ClassFileImporter;
import io.github.ppzxc.strand.api.PolicyException;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

class PolicyExceptionArchRuleTest {
    @Test
    void productionClassesWithCurrentExceptionsPass() {
        assertDoesNotThrow(() -> PolicyExceptionArchRule.check(
                new ClassFileImporter().importClasses(CurrentException.class), LocalDate.parse("2026-11-25")));
    }

    @Test
    void expiredProductionTypeFails() {
        assertThrows(
                AssertionError.class,
                () -> PolicyExceptionArchRule.check(
                        new ClassFileImporter().importClasses(ExpiredProductionType.class),
                        LocalDate.parse("2026-11-26")));
    }

    @Test
    void expiredMethodExceptionFails() {
        assertThrows(
                AssertionError.class,
                () -> PolicyExceptionArchRule.check(
                        new ClassFileImporter().importClasses(ExpiredProductionMethod.class),
                        LocalDate.parse("2026-11-26")));
    }

    @Test
    void everyRepeatedExceptionIsChecked() {
        assertThrows(
                AssertionError.class,
                () -> PolicyExceptionArchRule.check(
                        new ClassFileImporter().importClasses(RepeatedProductionExceptions.class),
                        LocalDate.parse("2026-11-26")));
    }

    @PolicyException(reviewBy = "2026-11-25")
    static class CurrentException {}

    @PolicyException(reviewBy = "2026-11-25")
    static class ExpiredProductionType {}

    static class ExpiredProductionMethod {
        @PolicyException(reviewBy = "2026-11-25")
        void operation() {}
    }

    @PolicyException(reviewBy = "2026-12-01")
    @PolicyException(reviewBy = "2026-11-25")
    static class RepeatedProductionExceptions {}
}
