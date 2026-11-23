import net.ltgt.gradle.errorprone.errorprone
import net.ltgt.gradle.nullaway.nullaway

plugins {
  id("strand.library-conventions")
  `java-test-fixtures`
}

description = "strand: 바이트 인코딩·디코딩 규격의 Java 구현"

// 엔드포인트 구동 도구(EndpointDriver)를 core 테스트, netty 테스트, 적합성 러너가 함께 쓰도록 test fixtures로 낸다.
// test fixtures는 test 코드처럼 NullAway 대상이 아니다.
tasks.named<JavaCompile>("compileTestFixturesJava") {
  options.errorprone.nullaway { disable() }
}

// test fixtures는 발행하지 않는다. java-test-fixtures가 java 컴포넌트에 붙이는 변형을 빼지 않으면 POM과 Gradle module metadata에
// fixtures가 optional로 새어 나간다.
(components["java"] as AdhocComponentWithVariants).apply {
  withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
  withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }
  withVariantsFromConfiguration(configurations["testFixturesSourcesElements"]) { skip() }
}

// java-test-fixtures를 켜면 테스트 클래스패스에 main이 클래스 디렉터리가 아니라 jar로 들어온다. 클래스 디렉터리에서 core를 읽는 테스트
// (CoreKnowsNoUsageEnvironmentTest, StrandVersionTest)가 전과 같이 동작하도록 main 클래스 디렉터리를 jar보다 앞에 둔다.
tasks.test {
  classpath = files(sourceSets.test.get().output, sourceSets.main.get().output, classpath)
}
