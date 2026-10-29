plugins {
  `kotlin-dsl`
}

// 외부 Gradle 플러그인은 버전 카탈로그의 [plugins]에서 플러그인 마커 좌표로 끌어온다.
fun Provider<PluginDependency>.asDependency(): Provider<String> = map {
  val version = it.version.requiredVersion
  check(version.isNotEmpty()) { "플러그인 ${it.pluginId}의 버전이 libs.versions.toml에 없다" }
  "${it.pluginId}:${it.pluginId}.gradle.plugin:$version"
}

dependencies {
  implementation(libs.plugins.errorprone.asDependency())
  implementation(libs.plugins.nullaway.asDependency())
  implementation(libs.plugins.spotless.asDependency())
  implementation(libs.plugins.maven.publish.asDependency())
  implementation(libs.plugins.japicmp.asDependency())
  // 플러그인 0.4.6이 끌어오는 japicmp 코어(0.23.1) 대신 카탈로그에 고정한 코어를 쓴다. 플러그인은 코어를 자기 클래스패스에서 읽는다.
  implementation(libs.japicmp)

  testImplementation(gradleTestKit())
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testImplementation(libs.assertj.core)
  testRuntimeOnly(libs.junit.platform.launcher)
}

// TestKit 기능 테스트(JDK 25에서만 돈다). 배포 산출물이 아니라 --release 8 대상이 아니다.
tasks.test {
  useJUnitPlatform()
  testLogging {
    exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
  }
}
