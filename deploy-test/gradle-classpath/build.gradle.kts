// strand 빌드와 분리된 외부 소비자. 입력은 -PstrandVersion(필수, 배포 버전이자 기대 버전)과
// -PrepoUrl(기본 mavenLocal)과 환경 변수 STRAND_REPO_TOKEN(선택, Bearer 토큰)이다.
plugins {
  application
}

val strandVersion = providers.gradleProperty("strandVersion").get()

apply(from = "../shared/repositories.gradle.kts")

java {
  toolchain { languageVersion = JavaLanguageVersion.of(25) }
}

// 소비자 코드는 Java 11 수준이다. 실행은 JDK 25다.
tasks.compileJava { options.release = 11 }

sourceSets.main { java.srcDir("../shared/src/main/java") }

dependencies {
  implementation("io.github.ppzxc:strand-core:$strandVersion")
  implementation("io.github.ppzxc:strand-nio:$strandVersion")
  implementation("io.github.ppzxc:strand-netty:$strandVersion")
  // strand-netty는 Netty를 compileOnlyApi로 의존해 POM에 싣지 않는다. 소비자가 Netty를 직접 의존한다.
  implementation("io.netty:netty-transport:4.1.101.Final")
}

application {
  mainClass = "strand.deploytest.Main"
}

// 기대 버전은 기본적으로 받아 온 버전이다. 비교 경로 자체를 시험할 때만 -PexpectedVersion, -PexpectedNioVersion, -PexpectedNettyVersion으로 덮어쓴다.
tasks.run {
  args(
    providers.gradleProperty("expectedVersion").orElse(strandVersion).get(),
    providers.gradleProperty("expectedNioVersion").orElse(strandVersion).get(),
    providers.gradleProperty("expectedNettyVersion").orElse(strandVersion).get(),
  )
}
