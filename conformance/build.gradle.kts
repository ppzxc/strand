plugins {
  id("strand.internal-conventions")
}

dependencies {
  testImplementation(project(":core"))
  testImplementation(libs.gson)
}

// conformance/vectors/ 는 외부 소비자와의 계약 경로다. test 리소스 디렉터리로 추가한다.
sourceSets.test {
  resources.srcDir("vectors")
}
