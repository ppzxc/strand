import java.nio.file.Path
import net.sourceforge.pmd.PMDConfiguration
import net.sourceforge.pmd.PmdAnalysis
import com.tngtech.archunit.core.importer.ClassFileImporter
import io.github.ppzxc.strand.quality.PolicyExceptionArchRule
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

  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val productionClassFiles: ConfigurableFileCollection

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
      pmd.addRuleSet(pmd.newRuleSetLoader().loadFromResource("handler-logging-ruleset.xml"))
      sourceFiles.files.forEach { pmd.files().addFile(it.toPath()) }
      pmd.performAnalysisAndCollectReport()
    }
    val current = report.violations
      .filter { it.rule.name == "PolicyExceptionSuppression" }
      .map { violation ->
      val path = Path.of(violation.fileId.absolutePath).normalize()
      "${root.relativize(path)}:${violation.beginLine}:${violation.description}"
    }.sorted()
    val approved = baseline.get().asFile.readLines().filter { it.isNotBlank() && !it.startsWith("#") }
    val newFindings = unapprovedPolicyFindings(current, approved)
    if (newFindings.isNotEmpty()) {
      throw GradleException(
        "새로 승인되지 않은 검사 억제 ${newFindings.size}개:\n" +
          newFindings.sorted().joinToString("\n") { "  - $it" },
      )
    }

    val classDirectories = productionClassFiles.files.filter { it.exists() }.map { it.toPath() }
    if (classDirectories.isNotEmpty()) {
      val classes = ClassFileImporter().importPaths(classDirectories)
      PolicyExceptionArchRule.check(classes, java.time.LocalDate.now(java.time.ZoneOffset.UTC))
    }
  }
}

internal fun unapprovedPolicyFindings(current: List<String>, baseline: List<String>): List<String> {
  val remaining = baseline.toMutableList()
  return current.filter { finding ->
    val baselineIndex = remaining.indexOf(finding)
    if (baselineIndex == -1) {
      true
    } else {
      remaining.removeAt(baselineIndex)
      false
    }
  }
}
