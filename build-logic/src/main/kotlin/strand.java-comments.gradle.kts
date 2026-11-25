import CheckJavaComments
import WriteJavaCommentBaseline

val repositoryJavaSources = fileTree(rootDir) {
  include("**/*.java")
  exclude("**/build/**", "**/.gradle/**", ".git/**")
}
val commentBaseline = layout.projectDirectory.file("config/java-comment-baseline.txt")

tasks.register<WriteJavaCommentBaseline>("updateJavaCommentBaseline") {
  group = "verification"
  description = "Rewrites the registered existing Java comments after an intentional baseline review."
  sourceFiles.from(repositoryJavaSources)
  baselineFile.set(commentBaseline)
}

val checkJavaComments = tasks.register<CheckJavaComments>("checkJavaComments") {
  group = "verification"
  description = "Fails when repository Java sources contain comments absent from the baseline."
  sourceFiles.from(repositoryJavaSources)
  baselineFile.set(commentBaseline)
}

tasks.named("check") {
  dependsOn(checkJavaComments)
}
