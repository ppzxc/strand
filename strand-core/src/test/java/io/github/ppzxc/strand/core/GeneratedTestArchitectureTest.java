package io.github.ppzxc.strand.core;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import org.instancio.junit.InstancioExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(InstancioExtension.class)
class GeneratedTestArchitectureTest {
    @Test
    void instancioTestsRegisterTheSeedReportingExtension() {
        ArchRule rule = classes()
                .should(new ArchCondition<JavaClass>("register the seed-reporting extension when using Instancio") {
                    @Override
                    public void check(JavaClass type, ConditionEvents events) {
                        boolean usesInstancio = type.getDirectDependenciesFromSelf().stream()
                                .anyMatch(dependency -> dependency
                                        .getTargetClass()
                                        .getPackageName()
                                        .startsWith("org.instancio"));
                        if (usesInstancio && !type.isAnnotatedWith(ExtendWith.class)) {
                            events.add(SimpleConditionEvent.violated(
                                    type, type.getName() + " must register InstancioExtension"));
                        }
                    }
                });

        rule.check(new ClassFileImporter().importPackages("io.github.ppzxc.strand"));
    }
}
