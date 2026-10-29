import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
  `java-library`
  id("com.diffplug.spotless")
}

private val catalog = versionCatalogs.named("libs")

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
  options.compilerArgs.add("-Xlint:-options")
}


dependencies {
  testImplementation(platform(catalog.findLibrary("junit-bom").get()))
  testImplementation(catalog.findLibrary("junit-jupiter").get())
  testImplementation(catalog.findLibrary("assertj-core").get())
  testRuntimeOnly(catalog.findLibrary("junit-platform-launcher").get())
}

tasks.test {
  useJUnitPlatform()
  testLogging {
    exceptionFormat = TestExceptionFormat.FULL
  }
}

spotless {
  java {
    target("src/**/*.java")
    palantirJavaFormat(catalog.findVersion("palantir-java-format").get().requiredVersion)
  }
}
