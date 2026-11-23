import net.ltgt.gradle.errorprone.errorprone

// 전송 모듈. core와 공통 앱 API(strand-api)에만 의존하고 함께 배포한다. Netty는 앱이 제공하므로 컴파일에만 쓰고 런타임 의존성에는 넣지 않는다.
plugins {
  id("strand.library-conventions")
}

description = "strand: strand-core 규격을 Netty 채널 위에서 주고받는 Netty 전송 구현"

// 컴파일 기준은 검증한 하한(4.1.101.Final)이다. 테스트는 -PnettyVersion=<버전>으로 지원 릴리스를 바꿔 반복 실행한다(docs/research/netty-progress-verification.md).
val nettyCompileVersion = "4.1.101.Final"
val nettyVersion = providers.gradleProperty("nettyVersion").orElse(nettyCompileVersion)

dependencies {
  api(project(":strand-core"))
  api(project(":strand-api"))
  // 공개 API가 Netty 타입(Future, EventLoopGroup)을 노출한다. 런타임에는 앱의 Netty가 쓰인다.
  compileOnlyApi("io.netty:netty-transport:$nettyCompileVersion")
  // WebSocket 전송을 고른 호출만 쓰는 코덱. 공개 API에 타입을 노출하지 않으므로 컴파일에만 쓰고 앱이 제공한다(TCP만 쓰면 없어도 된다).
  compileOnly("io.netty:netty-codec-http:$nettyCompileVersion")
  testImplementation(testFixtures(project(":strand-core")))
  testImplementation("io.netty:netty-transport:${nettyVersion.get()}")
  testImplementation("io.netty:netty-codec-http:${nettyVersion.get()}")
}

// 공개 API를 확정하거나 첫 배포를 해서 기준선 jar가 생기면 다시 켠다(strand-nio와 같다).
tasks.named("japicmp") {
  enabled = false
}

tasks.test {
  systemProperty("strand.nettyVersion", nettyVersion.get())
}

// WebSocket 코덱(netty-codec-http)은 앱이 제공하므로 TCP만 쓰는 사용자의 클래스패스에는 없다. 코덱을 뺀 클래스패스에서 TCP가 그대로 동작하고
// WebSocket 선택만 분명한 오류로 실패하는지 따로 돌려 확인한다.
val testWithoutCodec = tasks.register<Test>("testWithoutCodec") {
  group = "verification"
  description = "netty-codec-http 없이 TCP 동작과 WebSocket 선택 오류를 확인한다."
  testClassesDirs = sourceSets.test.get().output.classesDirs
  classpath = sourceSets.test.get().runtimeClasspath.filter { !it.name.startsWith("netty-codec-http") }
  useJUnitPlatform()
  filter { includeTestsMatching("*NoCodecTest") }
  systemProperty("strand.nettyVersion", nettyVersion.get())
}

tasks.test {
  filter { excludeTestsMatching("*NoCodecTest") }
}

tasks.check {
  dependsOn(testWithoutCodec)
}
