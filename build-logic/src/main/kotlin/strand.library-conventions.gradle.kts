import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SourcesJar
import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway
import org.gradle.external.javadoc.StandardJavadocDocletOptions

plugins {
  id("strand.java-conventions")
  id("net.ltgt.errorprone")
  id("net.ltgt.nullaway")
  jacoco
  id("com.vanniktech.maven.publish")
}

private val catalog = libsCatalog
private val artifactName = "strand-${project.name}"
private val moduleName = "io.github.ppzxc.strand.${project.name}"
private val minimumCoverage = "0.90".toBigDecimal()

dependencies {
  errorprone(catalog.library("errorprone-core"))
  errorprone(catalog.library("nullaway"))
  compileOnlyApi(catalog.library("jspecify"))
}

nullaway {
  onlyNullMarked = true
  // @NullMarked가 없는 패키지는 OnlyNullMarked에서 검사 대상이 아니라 조용히 넘어간다. 누락을 오류로 만든다.
  requireExplicitNullMarking { error() }
}

// main: 경고 하나도 허용하지 않는다. (test 컴파일은 java-conventions에서 -Werror 없이 린트 경고만 보여 준다.)
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
  // 리스너는 이 태스크의 LoggingManager에만 붙으므로 컴파일이 실패해 doLast가 안 돌아도 태스크가 끝나면
  // Gradle이 정리한다. 컴파일 오류로 실패하면 경고 수집·판정은 건너뛰지만 그 빌드는 이미 실패다.
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
  archivesName = artifactName
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
      "Implementation-Title" to artifactName,
      "Implementation-Version" to project.version.toString(),
    )
  }
}

jacoco {
  toolVersion = catalog.version("jacoco")
}

tasks.jacocoTestCoverageVerification {
  violationRules {
    rule {
      limit {
        counter = "LINE"
        value = "COVEREDRATIO"
        minimum = minimumCoverage
      }
      limit {
        counter = "BRANCH"
        value = "COVEREDRATIO"
        minimum = minimumCoverage
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
  val jspecify = catalog.library("jspecify").get()
  allowedCompile.add("${jspecify.module.group}:${jspecify.module.name}:${jspecify.versionConstraint.requiredVersion}")
  passedMarker.set(layout.buildDirectory.file("verifyDependencies/ok"))
}

tasks.check {
  dependsOn(verifyDependencies, tasks.jacocoTestCoverageVerification, tasks.javadoc)
}

// 배포: Central Portal USER_MANAGED(VALIDATED에서 멈추고 사람이 Publish). jar, sources jar, javadoc jar.
mavenPublishing {
  publishToMavenCentral(automaticRelease = false)
  configure(JavaLibrary(javadocJar = JavadocJar.Javadoc(), sourcesJar = SourcesJar.Sources()))
  // artifactId 기본값은 프로젝트 이름("core")이라 좌표를 명시한다.
  coordinates(project.group.toString(), artifactName, project.version.toString())

  // 서명 키(in-memory, CI의 ORG_GRADLE_PROJECT_signingInMemoryKey)가 비어 있지 않을 때만 서명한다.
  // signAllPublications()는 SNAPSHOT이 아닌 버전에서 서명 자격이 없으면 빌드를 실패시킨다
  // (vanniktech 문서, 확인: publishToMavenLocal이 "No configured signatory"로 실패). 그래서 키 없는 로컬 배포는 무서명으로 둔다.
  if (!providers.gradleProperty("signingInMemoryKey").orNull.isNullOrBlank()) {
    signAllPublications()
  }

  pom {
    name = artifactName
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
