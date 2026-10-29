import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
  `java-library`
  id("com.diffplug.spotless")
}

private val catalog = libsCatalog

repositories {
  mavenCentral()
}

java {
  toolchain {
    languageVersion = JavaLanguageVersion.of(25)
  }
}

tasks.withType<JavaCompile>().configureEach {
  options.release = 8
  options.encoding = "UTF-8"
}

tasks.named<JavaCompile>("compileTestJava") {
  // test는 -Werror 없이 린트 경고만 보여 준다. release 8 관련 [options] 경고만 끈다.
  options.compilerArgs.add("-Xlint:-options")
}

dependencies {
  testImplementation(platform(catalog.library("junit-bom")))
  testImplementation(catalog.library("junit-jupiter"))
  testImplementation(catalog.library("assertj-core"))
  testRuntimeOnly(catalog.library("junit-platform-launcher"))
}

tasks.test {
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
