// 기준선 태그의 적합성 벡터를 현재 코드로 다시 실행한다. PATCH만 올랐으면 실패 1건도 빌드 실패, MINOR 이상이면 보고만 한다.
plugins {
  java
  id("strand.compat-baseline")
}

val compat = extensions.getByType<StrandCompatExtension>()
val baselineVersion = compatBaselineVersion()
val patchOnly = compatPatchOnly().orElse(false)

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
  val hasBaseline = compatHasBaseline()
  onlyIf { hasBaseline.get() }

  // 결과를 바꾸는 값은 모두 입력으로 선언한다. 안 그러면 MINOR로 돈 결과가 PATCH에서도 UP-TO-DATE로 재사용된다.
  val vectorsDir = extractBaselineVectors.flatMap { it.outputDir }
  inputs.dir(vectorsDir).withPropertyName("baselineVectors").withPathSensitivity(PathSensitivity.RELATIVE)
  val patchOnlyProvider = patchOnly
  val vectorsPath = compat.vectorsPath
  val testFilter = compat.vectorTestFilter
  inputs.property("patchOnly", patchOnlyProvider)
  inputs.property("vectorsPath", vectorsPath)
  inputs.property("vectorTestFilter", testFilter)

  // 스크립트 객체를 잡지 않도록 지역 값만 쓴다(configuration cache). 값은 실행 시점에 읽는다.
  doFirst {
    ignoreFailures = !patchOnlyProvider.get()
    filter.includeTestsMatching(testFilter.get())
    systemProperty(
      "strand.conformance.vectors.dir",
      vectorsDir.get().asFile.resolve(vectorsPath.get()).absolutePath,
    )
  }
}

tasks.named("check") {
  dependsOn(baselineVectorTest)
}
