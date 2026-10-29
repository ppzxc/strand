plugins {
  id("strand.internal-conventions")
  id("strand.vector-compat-conventions")
}

dependencies {
  testImplementation(project(":core"))
  testImplementation(libs.gson)
}

// 스펙이 명시한 외부 소비자 계약: conformance/vectors/ 를 test 리소스 디렉터리로 둔다.
sourceSets.test {
  resources.srcDir("vectors")
}

tasks.test {
  // 러너는 작업 디렉터리가 아니라 이 프로퍼티로 벡터 디렉터리를 찾는다. (다른 디렉터리의 벡터를 돌릴 때도 같은 프로퍼티를 쓴다.)
  systemProperty("strand.conformance.vectors.dir", layout.projectDirectory.dir("vectors").asFile.absolutePath)
}
