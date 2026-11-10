import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway

// 전송 모듈. core에만 의존하고 core와 함께 배포한다. 의존성 0 검사는 같은 저장소의 strand-core만 예외로 둔다.
plugins {
  id("strand.library-conventions")
  `java-test-fixtures`
}

dependencies {
  api(project(":strand-core"))
  testFixturesApi(project(":strand-core"))
  testFixturesCompileOnly(libsCatalog.library("jspecify"))
  // Passage 계약 테스트(PassageContract)를 다른 통로 구현이 상속해 쓰게 fixtures로 낸다.
  testFixturesApi(platform(libsCatalog.library("junit-bom")))
  testFixturesApi(libsCatalog.library("junit-jupiter"))
  testFixturesApi(libsCatalog.library("assertj-core"))
}

// 공개 API가 모두 internal 패키지라 japicmp가 비교할 대상이 없고, 아직 배포한 적 없어 기준선 jar도 없다.
// 공개 API를 확정하거나 첫 배포를 해서 기준선 jar가 생기면 다시 켠다.
tasks.named("japicmp") {
  enabled = false
}

// test fixtures는 test 코드처럼 NullAway 대상이 아니다.
tasks.named<JavaCompile>("compileTestFixturesJava") {
  options.errorprone.nullaway { disable() }
}
