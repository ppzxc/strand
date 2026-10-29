// 기준선 태그의 적합성 벡터를 현재 코드로 다시 실행한다. PATCH만 올랐으면 실패 1건도 빌드 실패, MINOR 이상이면 보고만 한다.
plugins {
  java
  id("strand.compat-baseline")
}

val compat = extensions.getByType<StrandCompatExtension>()
val baselineVersion = compatBaselineVersion()
val patchOnly = compatPatchOnly()

val extractBaselineVectors = tasks.register<ExtractBaselineVectors>("extractBaselineVectors") {
  group = "verification"
  description = "기준선 태그의 벡터 디렉터리를 git에서 꺼낸다."
  baseline.set(baselineVersion)
  vectorsPath.set(compat.vectorsPath)
  gitDir.set(rootProject.layout.projectDirectory)
  outputDir.set(layout.buildDirectory.dir("compat/baseline-vectors"))
}

val baselineVectorTest = tasks.register<Test>("baselineVectorTest") {
  group = "verification"
  description = "기준선 태그의 벡터를 현재 코드로 실행한다."
  // test 태스크 자체는 돌리지 않는다. 같은 테스트 클래스와 클래스패스만 쓴다.
  val testSourceSet = sourceSets.test
  testClassesDirs = files(testSourceSet.map { it.output.classesDirs })
  classpath = files(testSourceSet.map { it.runtimeClasspath })
  useJUnitPlatform()
  val hasBaseline = baselineVersion.map { true }.orElse(false)
  onlyIf { hasBaseline.get() }
  dependsOn(extractBaselineVectors)
  ignoreFailures = !patchOnly.orElse(false).get()
  val vectorsDir = extractBaselineVectors.flatMap { it.outputDir }
  val relative = compat.vectorsPath
  doFirst {
    systemProperty(
      "strand.conformance.vectors.dir",
      vectorsDir.get().asFile.resolve(relative.get()).absolutePath,
    )
  }
}

afterEvaluate {
  baselineVectorTest { filter.includeTestsMatching(compat.vectorTestFilter.get()) }
}

tasks.named("check") {
  dependsOn(baselineVectorTest)
}
