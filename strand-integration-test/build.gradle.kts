plugins {
  id("strand.internal-conventions")
}

// Netty 진행 경계 검증은 릴리스를 -PnettyVersion=<버전>으로 바꿔 반복 실행한다(docs/research/netty-progress-verification.md).
val nettyVersion = providers.gradleProperty("nettyVersion")

dependencies {
  // 통합 테스트는 전송 모듈(strand-nio, core를 api로 노출) 위에서 돈다. 전송 모듈은 이 모듈에 의존하지 않는다.
  testImplementation(project(":strand-nio"))
  // 통로 시험 도구(PassageContract.writeFully)를 함께 쓴다.
  testImplementation(testFixtures(project(":strand-nio")))
  // 앱 공개 API 통합 수용 검증(#328)이 NIO와 Netty 전송을 모두 쓴다.
  testImplementation(project(":strand-netty"))
  testImplementation("io.netty:netty-transport:${nettyVersion.get()}")
}

tasks.test {
  systemProperty("strand.nettyVersion", nettyVersion.get())
}
