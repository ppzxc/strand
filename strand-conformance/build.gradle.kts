plugins {
  id("strand.internal-conventions")
  id("strand.vector-compat-conventions")
}

dependencies {
  testImplementation(project(":strand-core"))
  testImplementation(libs.gson)
}

// 규격이 명시한 외부 소비자 계약: strand-conformance/vectors/ 를 test 리소스 디렉터리로 둔다.
sourceSets.test {
  resources.srcDir("vectors")
}

tasks.test {
  // 규격을 읽는 검사(SpecCoverageTest 등)가 규격 변경에 다시 돌도록 입력으로 선언한다.
  inputs.file(rootProject.layout.projectDirectory.file("docs/specification/STRAND_SPECIFICATION.md"))
  // 러너는 작업 디렉터리가 아니라 이 프로퍼티로 벡터 디렉터리를 찾는다. (다른 디렉터리의 벡터를 돌릴 때도 같은 프로퍼티를 쓴다.)
  systemProperty("strand.conformance.vectors.dir", layout.projectDirectory.dir("vectors").asFile.absolutePath)
}
