package io.github.ppzxc.strand.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.tngtech.archunit.core.importer.ClassFileImporter;
import io.github.ppzxc.strand.api.PolicyException;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

class PolicyExceptionExpiryTest {
    @Test
    void anExceptionIsValidThroughItsReviewDate() {
        assertDoesNotThrow(() -> PolicyExceptionRules.check(
                new ClassFileImporter().importClasses(ValidPolicyException.class), LocalDate.parse("2026-11-25")));
    }

    @Test
    void anExceptionExpiresAtMidnightUtcAfterItsReviewDate() {
        assertThrows(
                AssertionError.class,
                () -> PolicyExceptionRules.check(
                        new ClassFileImporter().importClasses(ExpiredPolicyException.class),
                        LocalDate.parse("2026-11-26")));
    }

    @Test
    void aMethodExceptionIsCheckedToo() {
        assertThrows(
                AssertionError.class,
                () -> PolicyExceptionRules.check(
                        new ClassFileImporter().importClasses(MethodWithExpiredPolicyException.class),
                        LocalDate.parse("2026-11-26")));
    }

    @Test
    void everyRepeatedExceptionIsChecked() {
        assertThrows(
                AssertionError.class,
                () -> PolicyExceptionRules.check(
                        new ClassFileImporter().importClasses(RepeatedPolicyExceptions.class),
                        LocalDate.parse("2026-11-26")));
    }

    @PolicyException(rule = "ARC-001", reason = "test", approvedBy = "reviewer", reviewBy = "2026-11-25")
    static class ValidPolicyException {}

    @PolicyException(rule = "ARC-001", reason = "test", approvedBy = "reviewer", reviewBy = "2026-11-25")
    static class ExpiredPolicyException {}

    static class MethodWithExpiredPolicyException {
        @PolicyException(rule = "ARC-001", reason = "test", approvedBy = "reviewer", reviewBy = "2026-11-25")
        void method() {}
    }

    @PolicyException(rule = "ARC-001", reason = "test", approvedBy = "reviewer", reviewBy = "2026-12-01")
    @PolicyException(rule = "ARC-002", reason = "test", approvedBy = "reviewer", reviewBy = "2026-11-25")
    static class RepeatedPolicyExceptions {}
}
