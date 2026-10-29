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
    languageVersion = JavaLanguageVersion.of(StrandBuild.TOOLCHAIN_JAVA_VERSION)
  }
}

tasks.withType<JavaCompile>().configureEach {
  options.release = StrandBuild.JAVA_RELEASE
  options.encoding = "UTF-8"
}

tasks.named<JavaCompile>("compileTestJava") {
  // test는 -Werror 없이 린트 경고만 보여 준다. release 8 관련 [options] 경고는 끈다.
  options.compilerArgs.add("-Xlint:all,-options")
}

dependencies {
  testImplementation(platform(catalog.library("junit-bom")))
  testImplementation(catalog.library("junit-jupiter"))
  testImplementation(catalog.library("assertj-core"))
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
