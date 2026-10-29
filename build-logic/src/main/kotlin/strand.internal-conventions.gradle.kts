// 배포하지 않는 모듈(conformance, examples)용 컨벤션.
// java-conventions의 toolchain·--release·test·Spotless만 적용한다.
// 배포 모듈 전용 규칙(Error Prone, NullAway, jacoco, javadoc, 의존성 0 검사, maven-publish)은 일부러 적용하지 않는다.
plugins {
  id("strand.java-conventions")
}
