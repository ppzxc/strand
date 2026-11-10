import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

/** main 컴파일 경고 실패(`compileJava`)가 프로젝트의 main 소스 경로에 실제로 연결되어 있는지의 기능 테스트. */
class MainWarningFailureTest {
  @TempDir
  lateinit var dir: File

  private fun fixture() =
    BuildFixture(dir, "strand.library-conventions", usesCompatExtension = false).also { it.buildScript("0.0.1") }

  @Test
  fun `main 소스의 경고는 compileJava를 실패시킨다`() {
    val f = fixture()
    f.source("p/package-info.java", NULL_MARKED)
    f.source("p/Raw.java", "package p;\npublic class Raw { java.util.List raw() { return new java.util.ArrayList(); } }\n")

    val result = f.fail("compileJava")

    assertThat(result.output).contains("main 컴파일 경고").contains("Raw.java")
  }

  @Test
  fun `경고 없는 main 소스는 compileJava가 성공한다`() {
    val f = fixture()
    f.source("p/package-info.java", NULL_MARKED)
    f.source("p/Clean.java", "package p;\npublic class Clean { }\n")

    val result = f.run("compileJava")

    assertThat(result.task(":compileJava")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
  }

  private companion object {
    const val NULL_MARKED = "@org.jspecify.annotations.NullMarked\npackage p;\n"
  }
}
