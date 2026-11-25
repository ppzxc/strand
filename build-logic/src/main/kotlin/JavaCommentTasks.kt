import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import net.sourceforge.pmd.PMDConfiguration
import net.sourceforge.pmd.PmdAnalysis
abstract class CheckJavaComments : DefaultTask() {
  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceFiles: ConfigurableFileCollection

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val baseline: RegularFileProperty

  @get:Internal
  abstract val projectRoot: DirectoryProperty

  @TaskAction
  fun checkComments() {
    val root = projectRoot.get().asFile
    val configuration = PMDConfiguration()
    val current = PmdAnalysis.create(configuration).use { pmd ->
      pmd.addRuleSet(pmd.newRuleSetLoader().loadFromResource("java-style-ruleset.xml"))
      sourceFiles.files.forEach { pmd.files().addFile(it.toPath()) }
      pmd.performAnalysisAndCollectReport().violations.map { violation ->
        val path = violation.fileId.absolutePath
          .let { java.nio.file.Path.of(it).normalize() }
          .let { root.toPath().relativize(it).toString().replace('\\', '/') }
        "$path:${violation.beginLine}: ${violation.rule.name}"
      }
    }
    val approved = baseline.get().asFile.readLines().filter { it.isNotBlank() && !it.startsWith("#") }.toSet()
    val findings = current.toSet() - approved
    if (findings.isNotEmpty()) {
      throw GradleException("Java 스타일 위반 ${findings.size}개:\n${findings.sorted().joinToString("\n") { "  - $it" }}")
    }
  }
}
