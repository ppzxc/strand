plugins {
  id("strand.internal-conventions")
}

dependencies {
  // 통합 테스트는 전송 모듈(strand-nio, core를 api로 노출) 위에서 돈다. 전송 모듈은 이 모듈에 의존하지 않는다.
  testImplementation(project(":strand-nio"))
  // 통로 시험 도구(PassageContract.writeFully)를 함께 쓴다.
  testImplementation(testFixtures(project(":strand-nio")))
}

dependencies {
  testImplementation("io.netty:netty-transport:4.1.101.Final")
}
