import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class RequiredErrorProneChecksTest {
  @TempDir
  lateinit var dir: File

  @Test
  fun `필수 Error Prone 위반은 compileJava를 실패시킨다`() {
    val f = BuildFixture(dir, "strand.library-conventions", usesCompatExtension = false).also { it.buildScript("0.0.1") }
    f.source("p/package-info.java", "@org.jspecify.annotations.NullMarked\npackage p;\n")
    f.source(
      "p/ArrayComparison.java",
      "package p;\npublic class ArrayComparison { boolean same(String[] left, String[] right) { return left.equals(right); } }\n",
    )

    val result = f.fail("compileJava")

    assertThat(result.output).contains("error: [ArrayEquals]")
    assertThat(result.task(":compileJava")!!.outcome).isEqualTo(TaskOutcome.FAILED)
  }

  @Test
  fun `내부 Java 컨벤션도 필수 Error Prone 검사를 실행한다`() {
    val f = BuildFixture(dir, "strand.internal-conventions", usesCompatExtension = false).also { it.buildScript("0.0.1") }
    f.source("p/ArrayComparison.java", "package p;\npublic class ArrayComparison { boolean same(String[] left, String[] right) { return left.equals(right); } }\n")

    val result = f.fail("compileJava")

    assertThat(result.output).contains("error: [ArrayEquals]")
    assertThat(result.task(":compileJava")!!.outcome).isEqualTo(TaskOutcome.FAILED)
  }
}
