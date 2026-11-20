plugins {
  id("strand.internal-conventions")
}

// Netty 앱 예제는 -PnettyVersion=<버전>으로 지원 릴리스를 바꿔 실행한다(docs/research/app-api-integration-acceptance.md).
val nettyVersion = providers.gradleProperty("nettyVersion").orElse("4.1.101.Final")

dependencies {
  // 예제는 전송 모듈(strand-nio, core를 api로 노출) 위에서 돈다.
  implementation(project(":strand-nio"))
  // 앱 공개 API 예제(NettyAppExample)는 Netty 전송 모듈과 앱이 제공하는 Netty를 쓴다.
  implementation(project(":strand-netty"))
  implementation("io.netty:netty-transport:${nettyVersion.get()}")
  // WebSocket 통로 테스트가 Passage 계약(PassageContract)을 상속한다.
  testImplementation(testFixtures(project(":strand-nio")))
}

// JDK 11 java.net.http를 쓰므로 core와 달리 release 11. 배포하지 않고 컴파일만 확인한다.
tasks.withType<JavaCompile>().configureEach {
  options.release = 11
}
