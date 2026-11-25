package io.github.ppzxc.strand.api;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.jupiter.api.Test;

class DtoRecordTest {
    @Test
    void java8DtosUseOnlyApprovedRecordExceptions() {
        recordRule(RequestOptions.class.getName(), true)
                .check(new ClassFileImporter().importClasses(RequestOptions.class));
        recordRule(ResourceUsage.class.getName(), true)
                .check(new ClassFileImporter().importClasses(ResourceUsage.class));
    }

    @Test
    void unregisteredNonRecordDtoFails() {
        assertThrows(
                AssertionError.class,
                () -> recordRule(UnregisteredDto.class.getName(), false)
                        .check(new ClassFileImporter().importClasses(UnregisteredDto.class)));
    }

    private static ArchRule recordRule(String dtoName, boolean approvedJava8Exception) {
        return classes()
                .that()
                .haveFullyQualifiedName(dtoName)
                .should(new ArchCondition<JavaClass>("be a record or approved Java 8 DTO") {
                    @Override
                    public void check(JavaClass item, ConditionEvents events) {
                        boolean valid = item.isRecord() || approvedJava8Exception;
                        events.add(new SimpleConditionEvent(
                                item, valid, item.getName() + " must be a record or approved Java 8 DTO"));
                    }
                })
                .because("registered DTOs use records unless ADR #474 approves a Java 8 declaration exception")
                .allowEmptyShould(true);
    }

    private static final class UnregisteredDto {}
}
