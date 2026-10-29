plugins {
  id("strand.internal-conventions")
}

dependencies {
  implementation(project(":core"))
}

// JDK 11 java.net.http를 쓰므로 core와 달리 release 11. 배포하지 않고 컴파일만 확인한다.
tasks.withType<JavaCompile>().configureEach {
  options.release = 11
}
