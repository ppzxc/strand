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

  private fun fixture(version: String): BuildFixture =
    BuildFixture(dir, "strand.api-compat-conventions").also {
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

    assertThat(result.output).contains("Could not find g:lib:0.0.1")
  }

  @Test
  fun `PATCH 증가에서 public API 메서드를 제거하면 실패한다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.0.1")
    f.publishBaselineJar("0.0.1", baselineSources)
    f.source("p/Api.java", api.replace("public void keep() {}", ""))

    val result = f.fail("check")

    assertThat(result.task(":japicmp")!!.outcome).isEqualTo(TaskOutcome.FAILED)
    assertThat(report()).contains("p.Api").contains("REMOVED METHOD").contains("keep()")
  }

  private fun report(): String = File(dir, "build/reports/japicmp/report.txt").readText()

  @Test
  fun `PATCH 증가에서 public API 시그니처를 바꾸면 실패한다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.0.1")
    f.publishBaselineJar("0.0.1", baselineSources)
    f.source("p/Api.java", api.replace("int a(int x)", "int a(long x)").replace("return x;", "return (int) x;"))

    val result = f.fail("check")

    assertThat(result.task(":japicmp")!!.outcome).isEqualTo(TaskOutcome.FAILED)
    assertThat(report()).contains("p.Api").contains("REMOVED METHOD").contains("a(int)")
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

  @Test
  fun `PATCH 증가에서 internal 하위 패키지를 지워도 성공한다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.0.1")
    val sub = "package p.internal.sub;\npublic class Deep { public void d() {} }"
    f.publishBaselineJar("0.0.1", baselineSources + ("p/internal/sub/Deep.java" to sub))
    // 현재 소스에는 p.internal.sub.Deep이 없다.

    val result = f.run("check")

    assertThat(result.task(":japicmp")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
  }

  @Test
  fun `숫자로 비교한다 0_0_10이 0_0_9보다 크다`() {
    val f = fixture("0.0.11")
    f.commitAndTag("0.0.9")
    f.git("tag", "0.0.10")

    assertThat(f.run("compatBaseline").output).contains("기준선 0.0.10")
  }

  @Test
  fun `숫자로 비교한다 version 1_0_0보다 작은 최댓값은 0_10_0이다`() {
    val f = fixture("1.0.0")
    f.commitAndTag("0.9.9")
    for (t in listOf("0.10.0", "1.0.0", "1.0.1")) f.git("tag", t)

    assertThat(f.run("compatBaseline").output).contains("기준선 0.10.0")
  }

  @Test
  fun `int 범위를 넘는 숫자 태그는 무시한다`() {
    val f = fixture("0.0.5")
    f.commitAndTag("0.0.1")
    f.git("tag", "99999999999.0.0")

    assertThat(f.run("compatBaseline").output).contains("기준선 0.0.1")
  }

  @Test
  fun `git 저장소 안에서 git이 실패하면 기준선 없음으로 삼키지 않고 빌드가 실패한다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.0.1")
    File(dir, ".git/config").writeText("[core\n  broken")

    val result = f.fail("compatBaseline")

    assertThat(result.output).contains("git tag --list")
  }

  @Test
  fun `git 저장소가 아니면 기준선 없음이다`() {
    val f = BuildFixture(dir, "strand.api-compat-conventions", gitRepository = false)
    f.buildScript("0.0.2")

    assertThat(f.run("compatBaseline").output).contains("기준선 없음")
  }

  @Test
  fun `git 실행 파일이 없으면 기준선 없음이다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.0.1")
    f.buildScript("0.0.2", "strandCompat { gitExecutable = \"${File(dir, "no-such-git").path}\" }")

    assertThat(f.run("compatBaseline").output).contains("기준선 없음")
  }

  @Test
  fun `git은 있는데 실행할 수 없으면 기준선 없음으로 삼키지 않고 빌드가 실패한다`() {
    val f = fixture("0.0.2")
    f.commitAndTag("0.0.1")
    // 실행 권한이 없는 파일이라 프로세스 시작이 EACCES(error=13)로 실패한다.
    val broken = File(dir, "broken-git").apply { writeText("#!/bin/sh\n"); setExecutable(false) }
    f.buildScript("0.0.2", "strandCompat { gitExecutable = \"${broken.path}\" }")

    val result = f.fail("compatBaseline")

    assertThat(result.output).contains("git을 실행하지 못했다")
  }
}
