import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

/** 의존성 0 검사(`verifyDependencies`)의 기능 테스트. 실제 `strand.library-conventions`를 적용한다. */
class DependencyAllowlistTest {
  @TempDir
  lateinit var dir: File

  private fun fixture(dependencies: String): BuildFixture =
    BuildFixture(dir, "strand.library-conventions", usesCompatExtension = false).also {
      it.buildScript("0.0.1", "dependencies { $dependencies }")
    }

  @Test
  fun `허용된 의존성만 있으면 성공한다`() {
    val f = fixture("")

    val result = f.run("verifyDependencies")

    assertThat(result.task(":verifyDependencies")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
  }

  @Test
  fun `허용 외 의존성을 추가하면 실패한다`() {
    val f = fixture("""compileOnly("com.google.code.gson:gson:2.15.0")""")

    val result = f.fail("verifyDependencies")

    assertThat(result.output).contains("의존성 0 검사 실패").contains("com.google.code.gson:gson:2.15.0")
  }

  private fun fixtureWithProjects(dependency: String): BuildFixture {
    val f = fixture("""implementation(project("$dependency"))""")
    f.write(
      "settings.gradle.kts",
      File(dir, "settings.gradle.kts").readText() + "include(\"strand-core\")\ninclude(\"other\")\n",
    )
    f.write("strand-core/build.gradle.kts", "plugins { id(\"strand.java-conventions\") }\n")
    f.write("other/build.gradle.kts", "plugins { id(\"strand.java-conventions\") }\n")
    return f
  }

  @Test
  fun `strand-core 프로젝트 의존은 허용한다`() {
    val f = fixtureWithProjects(":strand-core")

    val result = f.run("verifyDependencies")

    assertThat(result.task(":verifyDependencies")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
  }

  @Test
  fun `strand-core 외의 프로젝트 의존은 실패한다`() {
    val f = fixtureWithProjects(":other")

    val result = f.fail("verifyDependencies")

    assertThat(result.output).contains("의존성 0 검사 실패").contains("project ':other'")
  }
}
