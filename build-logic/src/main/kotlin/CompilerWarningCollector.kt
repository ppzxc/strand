import org.gradle.api.GradleException
import org.gradle.api.logging.StandardOutputListener
import java.io.Serializable
import java.time.LocalDate
import java.time.ZoneOffset

/**
 * javac 출력에서 경고 줄을 모아 `-Werror` 역할을 대신한다.
 * 승인된 진단은 컴파일 태스크, 소스셋, 타깃과 재검토일이 모두 맞을 때만 건너뛴다.
 *
 * 경로가 있는 경고는 [sourceRoots] 아래의 것만 센다. 위치 없는 경고는 이 컴파일 태스크에서 나온 것으로 보고 실패시킨다.
 *
 * `[options]` 경고는 태스크에서 `-Xlint:-options`로 끈다.
 *
 * 한계: javac 메시지의 `warning:` 접두어와 영문 본문에 의존한다. 컴파일 JVM 로케일은 gradle.properties의
 * `org.gradle.jvmargs`(`-Duser.language=en`)로 영어에 고정한다.
 */
class CompilerWarningCollector(
  private val sourceRoots: List<String>,
  private val taskName: String,
  private val sourceSet: String,
  private val targetRelease: Int,
  private val exceptions: List<CompilerWarningException>,
) : StandardOutputListener, Serializable {
  private val warnings = mutableListOf<String>()

  override fun onOutput(output: CharSequence) {
    output.lineSequence()
      .filter { "warning:" in it }
      .filter { line -> ownsWarning(line) }
      .forEach { warnings += it.trim() }
  }

  private fun ownsWarning(line: String): Boolean {
    val match = SOURCE_WARNING.find(line)
    val path = match?.groupValues?.get(1)?.replace('\\', '/')
    val diagnostic = (match?.groupValues?.get(2) ?: line.substringAfter("warning:")).trim()
    if (exceptions.any { it.applies(diagnostic, taskName, sourceSet, targetRelease) }) return false
    if (path == null) return true
    return sourceRoots.any { path.startsWith(it.replace('\\', '/').trimEnd('/') + "/") }
  }

  fun failIfAny() {
    if (warnings.isNotEmpty()) {
      throw GradleException(
        "main 컴파일 경고 ${warnings.size}건 (경고는 오류로 취급한다)\n" +
          warnings.joinToString("\n") { "  - $it" },
      )
    }
  }

  private companion object {
    const val serialVersionUID = 1L

    val SOURCE_WARNING = Regex("""^\s*(.+\.java):\d+: warning:\s*(.*)$""")
  }
}

data class CompilerWarningException(
  val diagnostic: String,
  val taskName: String,
  val sourceSet: String,
  val targetRelease: Int,
  val reason: String,
  val approvedBy: String,
  val reviewBy: LocalDate,
) : Serializable {
  init {
    require(reason.isNotBlank())
    require(approvedBy.isNotBlank())
  }

  fun applies(diagnostic: String, taskName: String, sourceSet: String, targetRelease: Int) =
    this.diagnostic == diagnostic &&
      this.taskName == taskName &&
      this.sourceSet == sourceSet &&
      this.targetRelease == targetRelease &&
      !LocalDate.now(ZoneOffset.UTC).isAfter(reviewBy)
}
