plugins {
  id("strand.internal-conventions")
}

dependencies {
  // 예제는 전송 모듈(strand-nio, core를 api로 노출) 위에서 돈다.
  implementation(project(":strand-nio"))
  // WebSocket 통로 테스트가 Passage 계약(PassageContract)을 상속한다.
  testImplementation(testFixtures(project(":strand-nio")))
}

// JDK 11 java.net.http를 쓰므로 core와 달리 release 11. 배포하지 않고 컴파일만 확인한다.
tasks.withType<JavaCompile>().configureEach {
  options.release = 11
}
