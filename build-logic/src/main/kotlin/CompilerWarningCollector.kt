import org.gradle.api.GradleException
import org.gradle.api.logging.StandardOutputListener
import java.io.Serializable

/**
 * javac 출력에서 경고 줄을 모아 `-Werror` 역할을 대신한다.
 * 알려진 무해한 경고([IGNORED])만 건너뛴다.
 */
class CompilerWarningCollector : StandardOutputListener, Serializable {
  private val warnings = mutableListOf<String>()

  override fun onOutput(output: CharSequence) {
    output.lineSequence()
      .filter { "warning:" in it }
      .filterNot { line -> IGNORED.any { it in line } }
      .forEach { warnings += it.trim() }
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

    // JSpecify @NullMarked + --release 8. 설명은 strand.library-conventions 참고.
    val IGNORED = listOf("unknown enum constant ElementType.MODULE")
  }
}
