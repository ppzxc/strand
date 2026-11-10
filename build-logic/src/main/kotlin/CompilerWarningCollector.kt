import org.gradle.api.GradleException
import org.gradle.api.logging.StandardOutputListener
import java.io.Serializable

/**
 * javac 출력에서 경고 줄을 모아 `-Werror` 역할을 대신한다.
 * 알려진 무해한 경고([KNOWN_BENIGN_WARNINGS])만 건너뛴다.
 *
 * Gradle의 로깅 리스너는 병렬로 도는 다른 태스크의 출력도 받는다. 그래서 `경로.java:줄: warning:` 형태의 줄은
 * 경로가 [sourceRoots] 아래일 때만 센다. 경로 없는 경고(전역 경고)는 출처를 알 수 없으니 센다.
 *
 * 한계: javac 메시지의 `warning:` 접두어와 영문 본문에 의존한다. 컴파일 JVM 로케일은 gradle.properties의
 * `org.gradle.jvmargs`(`-Duser.language=en`)로 영어에 고정한다.
 */
class CompilerWarningCollector(private val sourceRoots: List<String>) : StandardOutputListener, Serializable {
  private val warnings = mutableListOf<String>()

  override fun onOutput(output: CharSequence) {
    output.lineSequence()
      .filter { "warning:" in it }
      .filter { line -> ownsWarning(line) }
      .filterNot { line -> KNOWN_BENIGN_WARNINGS.any { it in line } }
      .forEach { warnings += it.trim() }
  }

  private fun ownsWarning(line: String): Boolean {
    val path = SOURCE_WARNING.find(line)?.groupValues?.get(1) ?: return true
    return sourceRoots.any { path.startsWith(it.trimEnd('/') + "/") }
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

    val SOURCE_WARNING = Regex("""^\s*(.+\.java):\d+: warning:""")

    // JSpecify @NullMarked + --release 8. 설명은 strand.library-conventions 참고.
    val KNOWN_BENIGN_WARNINGS = listOf("unknown enum constant ElementType.MODULE")
  }
}
