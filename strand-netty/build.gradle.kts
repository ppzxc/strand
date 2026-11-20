import net.ltgt.gradle.errorprone.errorprone

// 전송 모듈. core와 공통 앱 API(strand-api)에만 의존하고 함께 배포한다. Netty는 앱이 제공하므로 컴파일에만 쓰고 런타임 의존성에는 넣지 않는다.
plugins {
  id("strand.library-conventions")
}

description = "strand: strand-core 규격을 Netty 채널 위에서 주고받는 Netty 전송 구현"

// 컴파일 기준은 검증한 하한(4.1.101.Final)이다. 테스트는 -PnettyVersion=<버전>으로 지원 릴리스를 바꿔 반복 실행한다(docs/research/netty-progress-verification.md).
val nettyCompileVersion = "4.2.20.Final"
val nettyVersion = providers.gradleProperty("nettyVersion").orElse(nettyCompileVersion)

dependencies {
  api(project(":strand-core"))
  api(project(":strand-api"))
  // 공개 API가 Netty 타입(Future, EventLoopGroup)을 노출한다. 런타임에는 앱의 Netty가 쓰인다.
  compileOnlyApi("io.netty:netty-transport:$nettyCompileVersion")
  testImplementation("io.netty:netty-transport:${nettyVersion.get()}")
}

// 공개 API를 확정하거나 첫 배포를 해서 기준선 jar가 생기면 다시 켠다(strand-nio와 같다).
tasks.named("japicmp") {
  enabled = false
}

tasks.test {
  systemProperty("strand.nettyVersion", nettyVersion.get())
}
