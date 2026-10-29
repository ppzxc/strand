// 호환성 기준선: "version보다 작은 X.Y.Z git 태그 중 최댓값" 하나. japicmp와 벡터 재실행 플러그인이 공유한다.
val compat = extensions.create<StrandCompatExtension>("strandCompat")
compat.baselineRepository.convention(providers.gradleProperty("strand.compat.baselineRepository").orElse("central"))
compat.gitExecutable.convention("git")
compat.vectorsPath.convention("conformance/vectors")

tasks.register<CompatBaselineTask>("compatBaseline") {
  group = "verification"
  description = "호환성 기준선(version보다 작은 X.Y.Z 태그 중 최댓값)을 출력한다."
  baseline.set(compatBaselineVersion())
}
