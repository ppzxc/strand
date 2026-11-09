// 배포하지 않는 JMH 벤치마크 모듈용 컨벤션. 벤치마크는 CI에서 측정하지 않고 컴파일만 확인한다(#200).
// 측정은 로컬에서 ./gradlew :<모듈>:jmh로 돌린다.
plugins {
  id("strand.internal-conventions")
  id("me.champeau.jmh")
}

private val catalog = libsCatalog

jmh {
  jmhVersion = catalog.version("jmh")
  fork = 1
  warmup = "1s"
  warmupIterations = 3
  timeOnIteration = "2s"
  iterations = 5
  timeUnit = "ms"
  resultFormat = "JSON"
  // 일부만 돌릴 때: ./gradlew :<모듈>:jmh -PjmhIncludes=<정규식>
  providers.gradleProperty("jmhIncludes").orNull?.let { includes = listOf(it) }
}

tasks.check {
  dependsOn(tasks.named("jmhClasses"))
}
