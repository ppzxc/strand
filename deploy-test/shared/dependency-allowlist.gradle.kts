val allowedModules = setOf(
  "io.github.ppzxc:strand-core",
  "io.github.ppzxc:strand-nio",
  "io.github.ppzxc:strand-netty",
  "io.github.ppzxc:strand-api",
  "io.netty:netty-transport",
  "io.netty:netty-common",
  "io.netty:netty-buffer",
  "io.netty:netty-resolver",
  "org.jspecify:jspecify",
  "com.google.errorprone:error_prone_annotations",
)

val verifyResolvedDependencies = tasks.register("verifyResolvedDependencies") {
  group = "verification"
  val runtimeClasspath = configurations.getByName("runtimeClasspath")
  inputs.files(runtimeClasspath)
  doLast {
    val result = runtimeClasspath.incoming.resolutionResult
    val resolved = result.allComponents
      .filter { component -> component.id != result.rootComponent.get().id }
      .mapNotNull { component -> component.moduleVersion?.let { "${it.group}:${it.name}" } }
      .toSet()
    val unexpected = resolved - allowedModules
    check(unexpected.isEmpty()) { "Consumer dependency allowlist failed. Unexpected resolved modules: ${unexpected.sorted()}" }
  }
}

tasks.named("compileJava") { dependsOn(verifyResolvedDependencies) }
