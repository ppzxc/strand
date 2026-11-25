import org.assertj.core.api.Assertions.assertThatThrownBy
import org.gradle.api.GradleException
import org.junit.jupiter.api.Test
import java.time.LocalDate

/**
 * 병렬 빌드에서 다른 태스크의 출력이 섞여 들어와도 자기 소스의 경고만 센다.
 * (CI에서 strand-nio:compileJava가 병렬로 돈 strand-core:compileTestJava의 경고로 실패한 결함의 회귀 시험.)
 */
class CompilerWarningCollectorTest {
  private val mine = "/w/strand-nio/src/main/java"
  private val foreign =
    "/w/strand-core/src/test/java/p/ConnectionPhaseTest.java:219: warning: [ImmutableEnumChecker] enums should be immutable"
  private val own = "$mine/p/Foo.java:10: warning: [Xlint] something"

  private fun collector(
    taskName: String = "compileJava",
    sourceSet: String = "main",
    targetRelease: Int = 8,
    reviewBy: LocalDate = LocalDate.parse("2099-12-31"),
  ) =
    CompilerWarningCollector(
      sourceRoots = listOf(mine),
      taskName = taskName,
      sourceSet = sourceSet,
      targetRelease = targetRelease,
      exceptions =
        listOf(
          CompilerWarningException(
            "unknown enum constant ElementType.MODULE",
            "compileJava",
            "main",
            8,
            "JSpecify target is unavailable in Java 8.",
            "ppzxc",
            reviewBy,
          ),
          CompilerWarningException(
            "[options] source value 8 is obsolete and will be removed in a future release",
            "compileJava",
            "main",
            8,
            "Java 8 release options are intentional.",
            "ppzxc",
            reviewBy,
          ),
        ),
    )

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
  fun `경로 없는 승인되지 않은 경고는 실패시킨다`() {
    val c = collector()

    c.onOutput("warning: [deprecation] deprecated API\n")

    assertThatThrownBy { c.failIfAny() }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("deprecated API")
  }

  @Test
  fun `JSpecify 예외와 비슷한 다른 경고는 실패시킨다`() {
    val c = collector()

    c.onOutput("$mine/p/Foo.java:3: warning: unknown enum constant ElementType.MODULES\n")

    assertThatThrownBy { c.failIfAny() }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("MODULES")
  }

  @Test
  fun `정확한 진단의 승인 예외만 건너뛴다`() {
    val c = collector()

    c.onOutput("warning: unknown enum constant ElementType.MODULE\n")
    c.onOutput("warning: [options] source value 8 is obsolete and will be removed in a future release\n")

    c.failIfAny()
  }

  @Test
  fun `예외는 승인한 태스크와 Java 타깃에만 적용한다`() {
    val c = collector(taskName = "compileTestJava", sourceSet = "test", targetRelease = 11)

    c.onOutput("warning: unknown enum constant ElementType.MODULE\n")

    assertThatThrownBy { c.failIfAny() }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("ElementType.MODULE")
  }

  @Test
  fun `만료된 예외는 경고를 허용하지 않는다`() {
    val c = collector(reviewBy = LocalDate.parse("2020-01-01"))

    c.onOutput("warning: unknown enum constant ElementType.MODULE\n")

    assertThatThrownBy { c.failIfAny() }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("ElementType.MODULE")
  }

  @Test
  fun `자기 소스 경로의 알려진 무해한 경고는 건너뛴다`() {
    val c = collector()

    c.onOutput("$mine/p/Foo.java:3: warning: unknown enum constant ElementType.MODULE\n")

    c.failIfAny()
  }

  @Test
  fun `경로 구분자가 역슬래시여도 자기 소스의 경고를 센다`() {
    val c =
      CompilerWarningCollector(
        listOf("C:\\w\\strand-nio\\src\\main\\java"),
        "compileJava",
        "main",
        8,
        emptyList(),
      )

    c.onOutput("C:\\w\\strand-nio\\src\\main\\java\\p\\Foo.java:10: warning: [Xlint] something\n")

    assertThatThrownBy { c.failIfAny() }.isInstanceOf(GradleException::class.java)
  }
}
