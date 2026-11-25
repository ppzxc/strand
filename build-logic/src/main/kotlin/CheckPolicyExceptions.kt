import java.nio.file.Path
import net.sourceforge.pmd.PMDConfiguration
import net.sourceforge.pmd.PmdAnalysis
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction

abstract class CheckPolicyExceptions : DefaultTask() {
  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceFiles: ConfigurableFileCollection

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val baseline: RegularFileProperty

  @get:Internal
  abstract val projectRoot: org.gradle.api.file.DirectoryProperty

  @TaskAction
  fun verifySuppressions() {
    val root = projectRoot.get().asFile.toPath()
    val configuration = PMDConfiguration()
    val report = PmdAnalysis.create(configuration).use { pmd ->
      pmd.addRuleSet(pmd.newRuleSetLoader().loadFromResource("policy-exception-ruleset.xml"))
      sourceFiles.files.forEach { pmd.files().addFile(it.toPath()) }
      pmd.performAnalysisAndCollectReport()
    }
    val current = report.violations.map { violation ->
      val path = Path.of(violation.fileId.absolutePath).normalize()
      "${root.relativize(path)}:${violation.beginLine}"
    }.toSet()
    val approved = baseline.get().asFile.readLines().filter { it.isNotBlank() && !it.startsWith("#") }.toSet()
    val newFindings = unapprovedPolicyFindings(current, approved)
    if (newFindings.isNotEmpty()) {
      throw GradleException(
        "새로 승인되지 않은 검사 억제 ${newFindings.size}개:\n" +
          newFindings.sorted().joinToString("\n") { "  - $it" },
      )
    }
  }
}

internal fun unapprovedPolicyFindings(current: Set<String>, baseline: Set<String>): Set<String> =
  current - baseline
