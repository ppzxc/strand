import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.errorprone

plugins {
  `java-library`
  id("com.diffplug.spotless")
  id("net.ltgt.errorprone")
}

private val catalog = libsCatalog

repositories {
  mavenCentral()
}

java {
  toolchain {
    languageVersion = JavaLanguageVersion.of(StrandBuild.TOOLCHAIN_JAVA_VERSION)
  }
}

tasks.withType<JavaCompile>().configureEach {
  options.release = StrandBuild.JAVA_RELEASE
  options.encoding = "UTF-8"
  options.compilerArgs.add("-Xlint:all,-options")
  options.errorprone {
    check("ArrayEquals", CheckSeverity.ERROR)
    check("ArrayToString", CheckSeverity.ERROR)
    check("EqualsIncompatibleType", CheckSeverity.ERROR)
    check("EqualsHashCode", CheckSeverity.ERROR)
    check("ModifyCollectionInEnhancedForLoop", CheckSeverity.ERROR)
    check("ModifySourceCollectionInStream", CheckSeverity.ERROR)
    check("NarrowCalculation", CheckSeverity.ERROR)
    check("NarrowingCompoundAssignment", CheckSeverity.ERROR)
  }
}

tasks.named<JavaCompile>("compileTestJava") {
  // test는 -Werror 없이 린트 경고만 보여 준다.
}

dependencies {
  errorprone(catalog.library("errorprone-core"))
  testImplementation(platform(catalog.library("junit-bom")))
  testImplementation(catalog.library("junit-jupiter"))
  testImplementation(catalog.library("assertj-core"))
  testImplementation(catalog.library("instancio-core"))
  testImplementation(catalog.library("instancio-junit"))
  testRuntimeOnly(catalog.library("junit-platform-launcher"))
}

tasks.test {
  // 런타임 매트릭스: Gradle과 컴파일은 JDK 25 그대로 두고 테스트 JVM만 -PtestJavaVersion=<N>으로 바꾼다.
  providers.gradleProperty("testJavaVersion").orNull?.let { version ->
    systemProperty("strand.expectedJavaVersion", version)
    // 매트릭스 실행에서만 테스트 출력(java.version 증거)을 로그로 보인다.
    testLogging.showStandardStreams = true
    javaLauncher = javaToolchains.launcherFor {
      languageVersion = JavaLanguageVersion.of(version.toInt())
    }
  }
  useJUnitPlatform()
  testLogging {
    exceptionFormat = TestExceptionFormat.FULL
  }
}

spotless {
  java {
    palantirJavaFormat(catalog.version("palantir-java-format"))
  }
}
