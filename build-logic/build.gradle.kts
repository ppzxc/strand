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
}
