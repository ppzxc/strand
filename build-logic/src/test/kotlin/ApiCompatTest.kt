import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

/** 기준선 태스크와 japicmp 검사의 기능 테스트. 모든 빌드는 configuration cache를 켠 채 돈다. */
class ApiCompatTest {
  @TempDir
  lateinit var dir: File

  private val api = """
    package p;
    public class Api {
      public int a(int x) { return x; }
      public void keep() {}
    }
  """.trimIndent()
  private val internal = """
    package p.internal;
    public class Impl { public void f() {} }
  """.trimIndent()
  private val baselineSources = mapOf("p/Api.java" to api, "p/internal/Impl.java" to internal)

  private fun fixture(version: String): CompatFixture =
    CompatFixture(dir, "strand.api-compat-conventions").also {
      it.buildScript(version)
      it.source("p/Api.java", api)
      it.source("p/internal/Impl.java", internal)
    }

  @Test
  fun `기준선 태그가 없으면 두 검사를 건너뛰고 성공한다`() {
    val f = fixture("0.0.2")

    val result = f.run("check", "compatBaseline")

    assertThat(result.output).contains("기준선 없음")
    assertThat(result.task(":japicmp")!!.outcome).isEqualTo(TaskOutcome.SKIPPED)
  }

  @Test
  fun `version보다 큰 태그만 있으면 기준선 없음으로 처리한다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.5.0")

    val result = f.run("check", "compatBaseline")

    assertThat(result.output).contains("기준선 없음")
    assertThat(result.task(":japicmp")!!.outcome).isEqualTo(TaskOutcome.SKIPPED)
  }

  @Test
  fun `여러 태그 중 version보다 작은 최댓값을 고르고 X_Y_Z가 아닌 태그는 무시한다`() {
    val f = fixture("0.0.5")
    f.commitAndTag("0.0.1")
    for (t in listOf("0.0.3", "0.0.5", "0.0.9", "v0.0.4", "0.0.4-rc1", "release")) f.git("tag", t)

    val result = f.run("compatBaseline")

    assertThat(result.output).contains("기준선 0.0.3")
  }

  @Test
  fun `태그는 있는데 기준선 jar가 없으면 실패한다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.0.1")

    val result = f.fail("check")

    assertThat(result.output).contains("g:lib:0.0.1")
  }

  @Test
  fun `PATCH 증가에서 public API 메서드를 제거하면 실패한다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.0.1")
    f.publishBaselineJar("0.0.1", baselineSources)
    f.source("p/Api.java", api.replace("public void keep() {}", ""))

    val result = f.fail("check")

    assertThat(result.task(":japicmp")!!.outcome).isEqualTo(TaskOutcome.FAILED)
  }

  @Test
  fun `PATCH 증가에서 public API 시그니처를 바꾸면 실패한다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.0.1")
    f.publishBaselineJar("0.0.1", baselineSources)
    f.source("p/Api.java", api.replace("int a(int x)", "int a(long x)").replace("return x;", "return (int) x;"))

    val result = f.fail("check")

    assertThat(result.task(":japicmp")!!.outcome).isEqualTo(TaskOutcome.FAILED)
  }

  @Test
  fun `PATCH 증가에서 internal 패키지만 바꾸면 성공한다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.0.1")
    f.publishBaselineJar("0.0.1", baselineSources)
    f.source("p/internal/Impl.java", internal.replace("public void f() {}", "public void g() {}"))

    val result = f.run("check")

    assertThat(result.task(":japicmp")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
  }

  @Test
  fun `PATCH 증가에서 호환되는 API 추가는 성공한다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.0.1")
    f.publishBaselineJar("0.0.1", baselineSources)
    f.source("p/Api.java", api.replace("public void keep() {}", "public void keep() {} public void added() {}"))

    val result = f.run("check")

    assertThat(result.task(":japicmp")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
  }

  @Test
  fun `MINOR 증가에서 API를 깨도 보고만 하고 성공한다`() {
    val f = fixture("0.1.0")
    f.commitAndTag("0.0.1")
    f.publishBaselineJar("0.0.1", baselineSources)
    f.source("p/Api.java", api.replace("public void keep() {}", ""))

    val result = f.run("check")

    assertThat(result.task(":japicmp")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
    assertThat(File(dir, "build/reports/japicmp/report.txt").readText()).contains("keep")
  }
}
