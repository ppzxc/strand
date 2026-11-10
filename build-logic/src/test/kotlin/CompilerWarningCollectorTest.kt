import org.assertj.core.api.Assertions.assertThatThrownBy
import org.gradle.api.GradleException
import org.junit.jupiter.api.Test

/**
 * 병렬 빌드에서 다른 태스크의 출력이 섞여 들어와도 자기 소스의 경고만 센다.
 * (CI에서 strand-nio:compileJava가 병렬로 돈 strand-core:compileTestJava의 경고로 실패한 결함의 회귀 시험.)
 */
class CompilerWarningCollectorTest {
  private val mine = "/w/strand-nio/src/main/java"
  private val foreign =
    "/w/strand-core/src/test/java/p/ConnectionPhaseTest.java:219: warning: [ImmutableEnumChecker] enums should be immutable"
  private val own = "$mine/p/Foo.java:10: warning: [Xlint] something"

  private fun collector() = CompilerWarningCollector(listOf(mine))

  @Test
  fun `다른 소스 트리의 경고는 세지 않는다`() {
    val c = collector()

    c.onOutput("$foreign\n")

    c.failIfAny()
  }

  @Test
  fun `자기 소스의 경고는 실패시킨다`() {
    val c = collector()

    c.onOutput("$foreign\n$own\n")

    assertThatThrownBy { c.failIfAny() }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("1건")
      .hasMessageContaining("Foo.java:10")
  }

  @Test
  fun `경로 없는 경고는 어느 태스크의 것인지 알 수 없으므로 세지 않는다`() {
    val c = collector()

    c.onOutput("warning: unknown enum constant ElementType.MODULE\n")
    c.onOutput("warning: [options] source value 8 is obsolete and will be removed in a future release\n")
    c.onOutput("warning: [options] To suppress warnings about obsolete options, use -Xlint:-options.\n")

    c.failIfAny()
  }

  @Test
  fun `자기 소스 경로의 알려진 무해한 경고는 건너뛴다`() {
    val c = collector()

    c.onOutput("$mine/p/Foo.java:3: warning: unknown enum constant ElementType.MODULE\n")

    c.failIfAny()
  }

  @Test
  fun `경로 구분자가 역슬래시여도 자기 소스의 경고를 센다`() {
    val c = CompilerWarningCollector(listOf("C:\\w\\strand-nio\\src\\main\\java"))

    c.onOutput("C:\\w\\strand-nio\\src\\main\\java\\p\\Foo.java:10: warning: [Xlint] something\n")

    assertThatThrownBy { c.failIfAny() }.isInstanceOf(GradleException::class.java)
  }
}
