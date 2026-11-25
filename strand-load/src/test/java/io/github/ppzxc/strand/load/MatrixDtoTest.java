package io.github.ppzxc.strand.load;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.junit.jupiter.api.Test;

class MatrixDtoTest {
    @Test
    void matrixCellIsARecordInTheJava25LoadSourceSet() {
        JavaClasses classes = new ClassFileImporter().importPath("build/classes/java/main");

        classes()
                .that()
                .haveFullyQualifiedName("io.github.ppzxc.strand.load.Matrix$Cell")
                .should(new ArchCondition<>("be a record") {
                    @Override
                    public void check(com.tngtech.archunit.core.domain.JavaClass item, ConditionEvents events) {
                        events.add(
                                new SimpleConditionEvent(item, item.isRecord(), item.getName() + " is not a record"));
                    }
                })
                .allowEmptyShould(false)
                .check(classes);
    }
}
