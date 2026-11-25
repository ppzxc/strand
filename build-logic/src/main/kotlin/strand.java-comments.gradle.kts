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
  description = "Fails when repository Java sources contain comments."
  sourceFiles.from(repositoryJavaSources)
  projectRoot.set(layout.projectDirectory)
}

tasks.named("check") {
  dependsOn(checkJavaComments)
}
