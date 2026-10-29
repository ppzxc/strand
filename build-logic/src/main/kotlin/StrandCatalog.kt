import org.gradle.api.Project
import org.gradle.api.artifacts.MinimalExternalModuleDependency
import org.gradle.api.artifacts.VersionCatalog
import org.gradle.api.artifacts.VersionCatalogsExtension
import org.gradle.api.provider.Provider

/** 루트 `gradle/libs.versions.toml`의 단일 카탈로그. 프리컴파일 스크립트에서는 타입 안전 접근자가 없다. */
val Project.libsCatalog: VersionCatalog
  get() = extensions.getByType(VersionCatalogsExtension::class.java).named("libs")

fun VersionCatalog.library(alias: String): Provider<MinimalExternalModuleDependency> =
  findLibrary(alias).orElseThrow { IllegalStateException("libs 카탈로그에 라이브러리 '$alias'가 없다") }

fun VersionCatalog.version(alias: String): String =
  findVersion(alias).orElseThrow { IllegalStateException("libs 카탈로그에 버전 '$alias'가 없다") }.requiredVersion
