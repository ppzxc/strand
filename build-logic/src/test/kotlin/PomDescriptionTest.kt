import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

/** POM description 누락이 POM 생성 시점이 아니라 `check`에서 일찍 드러나는지의 기능 테스트. */
class PomDescriptionTest {
  @TempDir
  lateinit var dir: File

  private fun fixture(extra: String) =
    BuildFixture(dir, "strand.library-conventions", usesCompatExtension = false).also { it.buildScript("0.0.1", extra) }

  @Test
  fun `description이 없으면 verifyPomDescription이 실패한다`() {
    val result = fixture("").fail("verifyPomDescription")

    assertThat(result.output).contains("description이 없다")
  }

  @Test
  fun `description이 공백뿐이어도 실패한다`() {
    val result = fixture("description = \"  \"").fail("verifyPomDescription")

    assertThat(result.output).contains("description이 없다")
  }

  @Test
  fun `description이 있으면 통과한다`() {
    fixture("description = \"설명\"").run("verifyPomDescription")
  }

  @Test
  fun `check가 verifyPomDescription에 의존한다`() {
    val result = fixture("description = \"설명\"").run("check", "--dry-run")

    assertThat(result.output).contains(":verifyPomDescription")
  }
}
