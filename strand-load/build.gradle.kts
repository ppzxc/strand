import org.gradle.api.tasks.testing.Test

plugins {
  id("strand.load-conventions")
}

// netty 서버 전송 측정은 -PnettyVersion=<버전>으로 지원 릴리스를 바꾼다(다른 내부 모듈과 같은 방식).
val nettyVersion = providers.gradleProperty("nettyVersion").orElse("4.1.101.Final")

dependencies {
  implementation(project(":strand-nio"))
  implementation(project(":strand-netty"))
  implementation("io.netty:netty-transport:${nettyVersion.get()}")
  // WebSocket 전송(Transport.webSocket)은 앱이 netty-codec-http를 제공해야 쓸 수 있다.
  implementation("io.netty:netty-codec-http:${nettyVersion.get()}")
  implementation(libs.hdrhistogram)
  implementation(libs.gson)
}

application {
  mainClass = "io.github.ppzxc.strand.load.LoadCli"
}

tasks.withType<Test>().configureEach {
  // 하네스가 자식 프로세스를 띄울 때 테스트 JVM의 클래스 경로를 그대로 쓴다.
  jvmArgs("-Xmx512m")
}
