import CheckJavaComments
val repositoryJavaSources = fileTree(rootDir) {
  include("**/*.java")
  exclude(
    "**/build/**",
    "**/.gradle/**",
    ".git/**",
    "**/generated/**",
    "**/vendor/**",
    "**/third-party/**",
    "**/third_party/**",
  )
}

val checkJavaComments = tasks.register<CheckJavaComments>("checkJavaComments") {
  group = "verification"
  description = "Checks repository Java style, allowed comments and public API Javadoc."
  sourceFiles.from(repositoryJavaSources)
  baseline.set(layout.projectDirectory.file("docs/quality/java-style-baseline.txt"))
  projectRoot.set(layout.projectDirectory)
}

val checkPolicyExceptions = tasks.register<CheckPolicyExceptions>("checkPolicyExceptions") {
  group = "verification"
  description = "Fails when new Java suppressions lack same-declaration policy exceptions."
  sourceFiles.from(repositoryJavaSources)
  productionClassFiles.from(project.rootProject.layout.projectDirectory.dir("build-logic/build/classes/java/main"))
  dependsOn(project.gradle.includedBuild("build-logic").task(":compileJava"))
  val policyTask = this
  project.rootProject.subprojects.forEach { module ->
    module.plugins.withId("java") {
      val main = module.extensions.getByType<org.gradle.api.tasks.SourceSetContainer>().named("main")
      policyTask.productionClassFiles.from(main.map { it.output.classesDirs })
      policyTask.dependsOn(module.tasks.named("classes"))
    }
  }
  baseline.set(layout.projectDirectory.file("docs/quality/pmd-suppression-baseline.txt"))
  projectRoot.set(layout.projectDirectory)
}

tasks.named("check") {
  dependsOn(checkJavaComments)
  dependsOn(checkPolicyExceptions)
}
