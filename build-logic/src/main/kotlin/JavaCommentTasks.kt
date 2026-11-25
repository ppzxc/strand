import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
abstract class CheckJavaComments : DefaultTask() {
  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceFiles: ConfigurableFileCollection

  @get:Internal
  abstract val projectRoot: DirectoryProperty

  @TaskAction
  fun checkComments() {
    val root = projectRoot.get().asFile
    val findings = sourceFiles.files
      .sortedBy { it.relativeTo(root).invariantSeparatorsPath }
      .flatMap { file ->
        JavaCommentScanner.find(file.readText()).map { comment ->
          "${file.relativeTo(root).invariantSeparatorsPath}:${comment.line}: ${comment.kind.name.lowercase()} comment"
        }
      }
    if (findings.isNotEmpty()) {
      throw GradleException("금지된 Java 주석 ${findings.size}개:\n${findings.joinToString("\n") { "  - $it" }}")
    }
  }
}
