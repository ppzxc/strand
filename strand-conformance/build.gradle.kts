plugins {
  id("strand.internal-conventions")
  id("strand.vector-compat-conventions")
}

// 제품 netty 경로(WebSocket 코덱)를 돌리는 Netty 버전. -PnettyVersion=<버전>으로 바꿔 반복 실행한다(strand-netty와 같다).
val nettyVersion = providers.gradleProperty("nettyVersion")

dependencies {
  testImplementation(project(":strand-core"))
  // 적합성 netty 경로: 제품 WebSocket 통로(strand-netty)와 그 코덱을 EmbeddedChannel로 돌린다.
  testImplementation(project(":strand-netty"))
  testImplementation("io.netty:netty-transport:${nettyVersion.get()}")
  testImplementation("io.netty:netty-codec-http:${nettyVersion.get()}")
  testImplementation(testFixtures(project(":strand-core")))
  testImplementation(testFixtures(project(":strand-nio")))
  testImplementation(libs.gson)
}

// 규격이 명시한 외부 소비자 계약: strand-conformance/vectors/ 를 test 리소스 디렉터리로 둔다.
sourceSets.test {
  resources.srcDir("vectors")
}

tasks.test {
  // 규격을 읽는 검사(SpecCoverageTest 등)가 규격 변경에 다시 돌도록 입력으로 선언한다.
  inputs.file(rootProject.layout.projectDirectory.file("docs/specification/STRAND_SPECIFICATION.md"))
  inputs.file(rootProject.layout.projectDirectory.file("docs/specification/STRAND_CONFORMANCE_VECTORS.md"))
  inputs.file(rootProject.layout.projectDirectory.file("gradle.properties"))
  // 러너는 작업 디렉터리가 아니라 이 프로퍼티로 벡터 디렉터리를 찾는다. (다른 디렉터리의 벡터를 돌릴 때도 같은 프로퍼티를 쓴다.)
  systemProperty("strand.conformance.vectors.dir", layout.projectDirectory.dir("vectors").asFile.absolutePath)
  // 기본 검사는 실행 경로를 통합한다. `-Dstrand.conformance.execution.path=core|nio|netty`로 단일 경로를 강제할 수 있다.
  systemProperty(
    "strand.conformance.execution.path",
    System.getProperty("strand.conformance.execution.path", "integrated"),
  )
  // 규격 문서 위치도 벡터 디렉터리와 따로 넘긴다. 벡터 디렉터리를 다른 곳으로 바꿔도 규격 색인은 이 문서를 읽는다.
  systemProperty(
    "strand.conformance.spec.file",
    rootProject.layout.projectDirectory.file("docs/specification/STRAND_SPECIFICATION.md").asFile.absolutePath,
  )
}
