import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
abstract class CheckJavaComments : DefaultTask() {
  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceFiles: ConfigurableFileCollection

  @get:InputFile
  abstract val baselineFile: RegularFileProperty

  @TaskAction
  fun checkComments() {
    val root = project.rootDir
    val findings = JavaCommentBaseline.newComments(sourceFiles.files, root, baselineFile.get().asFile.readLines())
    if (findings.isNotEmpty()) {
      throw GradleException("신규 Java 주석 ${findings.size}개:\n${findings.joinToString("\n") { "  - $it" }}")
    }
  }
}

abstract class WriteJavaCommentBaseline : DefaultTask() {
  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceFiles: ConfigurableFileCollection

  @get:OutputFile
  abstract val baselineFile: RegularFileProperty

  @TaskAction
  fun writeBaseline() {
    val root = project.rootDir
    val output = baselineFile.get().asFile
    output.parentFile.mkdirs()
    output.writeText(JavaCommentBaseline.contents(sourceFiles.files, root))
  }
}
