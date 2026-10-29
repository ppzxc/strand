import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway
import org.gradle.external.javadoc.StandardJavadocDocletOptions

plugins {
  id("strand.java-conventions")
  id("net.ltgt.errorprone")
  id("net.ltgt.nullaway")
  jacoco
  `maven-publish`
}

private val catalog = versionCatalogs.named("libs")
private val moduleName = "io.github.ppzxc.strand.${project.name}"

dependencies {
  errorprone(catalog.findLibrary("errorprone-core").get())
  errorprone(catalog.findLibrary("nullaway").get())
  compileOnlyApi(catalog.findLibrary("jspecify").get())
}

nullaway {
  onlyNullMarked = true
  // @NullMarked가 없는 패키지는 OnlyNullMarked에서 검사 대상이 아니라 조용히 넘어간다. 누락을 오류로 만든다.
  requireExplicitNullMarking { error() }
}

// main: 경고 하나도 허용하지 않는다. test는 린트 경고만 보여 주고 실패시키지 않는다.
//
// `-Werror`를 직접 쓰지 못하는 이유: JSpecify의 @NullMarked는 @Target에 ElementType.MODULE(Java 9)을
// 가지고 있어서 `--release 8`로 컴파일하면 javac(JDK 21, 25 확인)가 "unknown enum constant
// ElementType.MODULE" 경고를 낸다. 이 경고는 -Xlint 범주가 아니라 끌 방법이 없고(-nowarn만 가능),
// -Werror와 함께 쓰면 모든 main 컴파일이 실패한다. 그래서 -Xlint:all,-options는 그대로 두고
// -Werror 대신 컴파일러 출력을 받아 이 경고 외의 경고가 있으면 태스크를 실패시킨다.
tasks.named<JavaCompile>("compileJava") {
  options.compilerArgs.add("-Xlint:all,-options")
  val collector = CompilerWarningCollector()
  doFirst {
    logging.addStandardOutputListener(collector)
    logging.addStandardErrorListener(collector)
  }
  doLast {
    logging.removeStandardOutputListener(collector)
    logging.removeStandardErrorListener(collector)
    collector.failIfAny()
  }
}

// test 코드는 NullAway 대상이 아니다. @NullMarked는 main 패키지에 붙어 있어 같은 이름의 test 패키지에도 번지므로 끈다.
tasks.named<JavaCompile>("compileTestJava") {
  options.errorprone.nullaway { disable() }
}

base {
  archivesName = "strand-${project.name}"
}

java {
  withSourcesJar()
  withJavadocJar()
}

tasks.javadoc {
  (options as StandardJavadocDocletOptions).apply {
    addStringOption("Xdoclint:all,-missing", "-quiet")
    addBooleanOption("Werror", true)
    encoding = "UTF-8"
  }
}

tasks.jar {
  manifest {
    attributes(
      "Automatic-Module-Name" to moduleName,
      "Implementation-Title" to "strand-${project.name}",
      "Implementation-Version" to project.version.toString(),
    )
  }
}

jacoco {
  toolVersion = "0.8.14"
}

tasks.jacocoTestReport {
  dependsOn(tasks.test)
}

tasks.jacocoTestCoverageVerification {
  dependsOn(tasks.test)
  violationRules {
    rule {
      limit {
        counter = "LINE"
        value = "COVEREDRATIO"
        minimum = "0.90".toBigDecimal()
      }
      limit {
        counter = "BRANCH"
        value = "COVEREDRATIO"
        minimum = "0.90".toBigDecimal()
      }
    }
  }
}

val verifyDependencies = tasks.register<VerifyDependencyAllowlist>("verifyDependencies") {
  group = "verification"
  description = "런타임 의존성 0, 컴파일 의존성은 JSpecify만 허용하는지 검사한다."
  fun resolved(configurationName: String): Provider<Set<String>> {
    val result = configurations.getByName(configurationName).incoming.resolutionResult
    return result.rootComponent.map { root ->
      result.allComponents.filter { it != root }.map { it.id.displayName }.toSet()
    }
  }
  modulePath.set(project.path)
  runtimeComponents.set(resolved("runtimeClasspath"))
  compileComponents.set(resolved("compileClasspath"))
  val jspecify = catalog.findLibrary("jspecify").get().get()
  allowedCompile.add("${jspecify.module.group}:${jspecify.module.name}:${jspecify.versionConstraint.requiredVersion}")
  marker.set(layout.buildDirectory.file("verifyDependencies/ok"))
}

tasks.check {
  dependsOn(verifyDependencies, tasks.jacocoTestCoverageVerification, tasks.javadoc)
}

publishing {
  publications {
    create<MavenPublication>("maven") {
      from(components["java"])
      artifactId = "strand-${project.name}"
      pom {
        name = "strand-${project.name}"
        description = "strand: 바이트 인코딩·디코딩 규격의 Java 구현"
        url = "https://github.com/ppzxc/strand"
        licenses {
          license {
            name = "The Apache License, Version 2.0"
            url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
            distribution = "repo"
          }
        }
        developers {
          developer {
            id = "ppzxc"
            name = "ppzxc"
            url = "https://github.com/ppzxc"
          }
        }
        scm {
          url = "https://github.com/ppzxc/strand"
          connection = "scm:git:https://github.com/ppzxc/strand.git"
          developerConnection = "scm:git:ssh://git@github.com/ppzxc/strand.git"
        }
      }
    }
  }
}
