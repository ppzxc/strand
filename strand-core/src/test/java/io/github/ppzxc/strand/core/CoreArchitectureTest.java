package io.github.ppzxc.strand.core;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;

class CoreArchitectureTest {
  @Test
  void coreDoesNotDependOnTransportModules() {
    JavaClasses classes = new ClassFileImporter().importPackages("io.github.ppzxc.strand.core");

    noClasses()
        .that()
        .resideInAPackage("io.github.ppzxc.strand.core..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage("io.github.ppzxc.strand.nio..", "io.github.ppzxc.strand.netty..")
        .check(classes);
  }

  @Test
  void coreImplementationStaysInInternalPackage() {
    JavaClasses classes = new ClassFileImporter().importPackages("io.github.ppzxc.strand.core");

    noClasses()
        .that()
        .resideInAPackage("io.github.ppzxc.strand.core")
        .should()
        .beAssignableTo(StrandVersion.class)
        .check(classes);
  }

  @Test
  void corePackagesHaveNoCycles() {
    JavaClasses classes = new ClassFileImporter().importPackages("io.github.ppzxc.strand.core");

    slices().matching("io.github.ppzxc.strand.core.(*)..").should().beFreeOfCycles().check(classes);
  }
}
