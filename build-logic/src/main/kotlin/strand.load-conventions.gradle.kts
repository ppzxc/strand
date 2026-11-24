// 배포하지 않는 부하 하네스 모듈(strand-load)용 컨벤션(#422).
// 제품 코드가 아니므로 Java release 8을 강제하지 않고 toolchain 버전으로 컴파일한다.
// 서버 쪽 strand-nio·strand-netty는 기존 release 8 산출물을 그대로 쓴다.
// 배포 모듈 전용 규칙(의존성 0 검사, maven-publish 등)은 적용하지 않는다.
plugins {
  id("strand.internal-conventions")
  application
}

tasks.withType<JavaCompile>().configureEach {
  options.release = StrandBuild.TOOLCHAIN_JAVA_VERSION
}
