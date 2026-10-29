import org.gradle.api.GradleException
import org.gradle.api.logging.StandardOutputListener
import java.io.Serializable

/**
 * javac 출력에서 경고 줄을 모아 `-Werror` 역할을 대신한다.
 * 알려진 무해한 경고([KNOWN_BENIGN_WARNINGS])만 건너뛴다.
 *
 * 한계: javac 메시지의 `warning:` 접두어와 영문 본문에 의존한다. 컴파일 JVM 로케일은 gradle.properties의
 * `org.gradle.jvmargs`(`-Duser.language=en`)로 영어에 고정한다.
 */
class CompilerWarningCollector : StandardOutputListener, Serializable {
  private val warnings = mutableListOf<String>()

  override fun onOutput(output: CharSequence) {
    output.lineSequence()
      .filter { "warning:" in it }
      .filterNot { line -> KNOWN_BENIGN_WARNINGS.any { it in line } }
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
    val KNOWN_BENIGN_WARNINGS = listOf("unknown enum constant ElementType.MODULE")
  }
}
