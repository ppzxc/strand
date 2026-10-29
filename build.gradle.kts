plugins {
  base
}

// build-logic의 TestKit 기능 테스트(호환성 검사, 의존성 0 검사)를 루트 check에 묶는다. JDK 25에서만 돌고 런타임 매트릭스에는 넣지 않는다.
tasks.check {
  dependsOn(gradle.includedBuild("build-logic").task(":test"))
}

// 런타임 매트릭스(CI의 JDK 8/11/17/21/25)가 실행할 테스트 모음. 모듈을 추가할 때 여기만 고친다.
// examples(release 11)와 build-logic은 일부러 뺀다.
tasks.register("runtimeMatrixTest") {
  group = "verification"
  description = "런타임 매트릭스 대상 모듈(core, conformance)의 테스트만 실행한다. -PtestJavaVersion=<N>과 함께 쓴다."
  dependsOn(":core:test", ":conformance:test")
}
