// strand 빌드와 분리된 외부 소비자. 입력은 -PstrandVersion(필수, 배포 버전이자 기대 버전)과
// -PrepoUrl(기본 mavenLocal)과 환경 변수 STRAND_REPO_TOKEN(선택, Bearer 토큰)이다.
plugins {
  application
}

val strandVersion = providers.gradleProperty("strandVersion").get()

apply(from = "../shared/repositories.gradle.kts")
apply(from = "../shared/dependency-allowlist.gradle.kts")

java {
  toolchain { languageVersion = JavaLanguageVersion.of(25) }
}

// 소비자 코드는 Java 11 수준이다. 실행은 JDK 25다.
tasks.compileJava { options.release = 11 }

// 모듈 경로 소비자만 module-info를 얹는다.
sourceSets.main { java.srcDirs("../shared/src/main/java", "../shared/module") }

dependencies {
  implementation("io.github.ppzxc:strand-core:$strandVersion")
  implementation("io.github.ppzxc:strand-nio:$strandVersion")
  implementation("io.github.ppzxc:strand-netty:$strandVersion")
  // strand-netty는 Netty를 compileOnlyApi로 의존하며 게시된 POM에는 compile 범위로 실린다. 소비자는 사용할 Netty 버전을 직접 지정한다.
  implementation("io.netty:netty-transport:4.1.101.Final")
}

application {
  mainModule = "strand.deploytest"
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
