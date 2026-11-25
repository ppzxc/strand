package io.github.ppzxc.strand.core;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;
import com.tngtech.archunit.lang.ArchRule;

class CoreArchitectureTest {
  @Test
  void coreHasNoTransportDependencies() {
    JavaClasses classes = new ClassFileImporter().importPath("build/classes/java/main");

    coreDoesNotDependOnTransportModules().check(classes);
  }

  @Test
  void transportDependencyViolationFailsTheArchitectureRule() {
    JavaClasses sample = new ClassFileImporter().importClasses(CoreTransportViolationSample.class);

    assertThrows(AssertionError.class, () -> coreDoesNotDependOnTransportModules().check(sample));
  }

  private static ArchRule coreDoesNotDependOnTransportModules() {
    return noClasses()
        .that()
        .resideInAPackage("io.github.ppzxc.strand.core..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage("io.github.ppzxc.strand.nio..", "io.github.ppzxc.strand.netty..");
  }

  @Test
  void coreImplementationStaysInInternalPackage() {
    JavaClasses classes = new ClassFileImporter().importPath("build/classes/java/main");

    noClasses()
        .that()
        .resideInAPackage("io.github.ppzxc.strand.core")
        .and()
        .areNotAssignableTo(StrandVersion.class)
        .should()
        .beAssignableTo(StrandVersion.class)
        .check(classes);
  }

  @Test
  void corePackagesHaveNoCycles() {
    JavaClasses classes = new ClassFileImporter().importPath("build/classes/java/main");

    slices().matching("io.github.ppzxc.strand.core.(*)..").should().beFreeOfCycles().check(classes);
  }
}
