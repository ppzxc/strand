import org.gradle.process.CommandLineArgumentProvider
import org.gradle.api.tasks.Input

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

// 배포 테스트 하네스 (a): mavenLocal에 배포한 core를 외부 소비자 4개가 받아 쓰는지 확인한다. 느려서 check에는 묶지 않는다.
// 소비자 JDK 25와 mvn(없으면 고정 버전을 받아 온다)이 필요하다.
// version은 provider로 넘겨 태스크 실행 때만 읽는다(구성 단계에서 get()하지 않는다).
// lambda는 스크립트 객체를 잡아 configuration cache가 직렬화하지 못하므로 클래스로 둔다.
class VersionArg(@get:Input val version: Provider<String>) : CommandLineArgumentProvider {
  override fun asArguments() = listOf(version.get())
}

val deployTestVersion = providers.gradleProperty("version")
tasks.register<Exec>("deployTest") {
  group = "verification"
  description = "~/.m2에 strand-core를 발행하고(정리하지 않는다) deploy-test/run.sh로 외부 소비자 4개를 돌린다(하네스 a)."
  dependsOn(":core:publishToMavenLocal")
  workingDir = layout.projectDirectory.asFile
  executable = layout.projectDirectory.file("deploy-test/run.sh").asFile.path
  argumentProviders.add(VersionArg(deployTestVersion))
}
