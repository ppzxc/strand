import me.champeau.gradle.japicmp.JapicmpTask

// 기준선 jar 대비 공개 API 호환성. PATCH만 올랐으면 위반 시 실패, MINOR 이상이면 보고만 한다.
plugins {
  `java-library`
  id("me.champeau.gradle.japicmp")
  id("strand.compat-baseline")
}

val compat = extensions.getByType<StrandCompatExtension>()
val baselineVersion = compatBaselineVersion()
val patchOnly = compatPatchOnly()

val baselineJar = configurations.create("compatBaselineJar") {
  isCanBeConsumed = false
  isCanBeResolved = true
  isTransitive = false
  description = "호환성 기준선 jar"
}

val artifactName = extensions.getByType<BasePluginExtension>().archivesName

dependencies.addProvider(
  baselineJar.name,
  baselineVersion.zip(artifactName) { version, name -> "${project.group}:$name:$version" },
)

afterEvaluate {
  repositories.exclusiveContent {
    forRepository {
      when (val repo = compat.baselineRepository.get()) {
        "central" -> repositories.mavenCentral()
        "mavenLocal" -> repositories.mavenLocal()
        else -> repositories.maven { url = uri(repo) }
      }
    }
    filter { includeModule(project.group.toString(), artifactName.get()) }
  }
}

val japicmp = tasks.register<JapicmpTask>("japicmp") {
  group = "verification"
  description = "기준선 jar 대비 core의 바이너리·소스 호환성을 비교한다. .internal은 제외한다."
  val hasBaseline = baselineVersion.map { true }.orElse(false)
  onlyIf { hasBaseline.get() }
  oldArchives.from(baselineJar)
  newArchives.from(tasks.named("jar"))
  packageExcludes.add("*.internal")
  packageExcludes.add("*.internal.*")
  ignoreMissingClasses = true
  onlyModified = true
  failOnModification = patchOnly.orElse(false)
  failOnSourceIncompatibility = patchOnly.orElse(false)
  txtOutputFile = layout.buildDirectory.file("reports/japicmp/report.txt")
}

tasks.named("check") {
  dependsOn(japicmp)
}
