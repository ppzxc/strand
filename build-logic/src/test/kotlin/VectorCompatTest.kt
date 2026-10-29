import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

/** 기준선 태그의 벡터를 현재 코드로 다시 실행하는 검사의 기능 테스트. */
class VectorCompatTest {
  @TempDir
  lateinit var dir: File

  // 현재 core 대역: 디렉터리의 json 중 "FAIL"이 들어 있는 벡터를 거부한다.
  private val runnerTest = """
    import java.nio.file.*;
    import java.util.stream.*;
    import org.junit.jupiter.api.Test;
    import static org.junit.jupiter.api.Assertions.fail;

    class VectorConformanceTest {
      @Test
      void vectors() throws Exception {
        String dir = System.getProperty("strand.conformance.vectors.dir");
        if (dir == null) return;
        try (Stream<Path> files = Files.list(Paths.get(dir))) {
          for (Path p : files.collect(Collectors.toList())) {
            if (new String(Files.readAllBytes(p)).contains("FAIL")) fail("rejected " + p.getFileName());
          }
        }
      }
    }
  """.trimIndent()

  private fun fixture(version: String): CompatFixture =
    CompatFixture(dir, "strand.vector-compat-conventions").also {
      it.buildScript(
        version,
        """
        dependencies {
          testImplementation(platform("org.junit:junit-bom:5.14.4"))
          testImplementation("org.junit.jupiter:junit-jupiter")
          testRuntimeOnly("org.junit.platform:junit-platform-launcher")
        }
        tasks.test {
          useJUnitPlatform()
          systemProperty("strand.conformance.vectors.dir", "${'$'}projectDir/conformance/vectors")
        }
        """.trimIndent(),
      )
      it.write("src/test/java/VectorConformanceTest.java", runnerTest)
    }

  private fun baselineVectors(f: CompatFixture, content: String, working: String = "ok") {
    f.write("conformance/vectors/v.json", content)
    f.commitAndTag("0.0.1")
    f.write("conformance/vectors/v.json", working)
  }

  @Test
  fun `기준선 태그가 없으면 벡터 재실행을 건너뛴다`() {
    val f = fixture("0.0.2")
    f.write("conformance/vectors/v.json", "ok")

    val result = f.run("check")

    assertThat(result.task(":baselineVectorTest")!!.outcome).isEqualTo(TaskOutcome.SKIPPED)
  }

  @Test
  fun `기준선 벡터가 모두 통과하면 성공하고 작업 트리의 벡터가 아니라 태그의 벡터를 쓴다`() {
    val f = fixture("0.0.2")
    baselineVectors(f, "ok", working = "현재 작업 트리의 벡터는 기준선과 다르다. FAIL")

    val result = f.run("baselineVectorTest")

    assertThat(result.task(":baselineVectorTest")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
  }

  @Test
  fun `PATCH 증가에서 기준선 벡터가 실패하면 빌드가 실패한다`() {
    val f = fixture("0.0.2")
    baselineVectors(f, "FAIL")

    val result = f.fail("check")

    assertThat(result.task(":baselineVectorTest")!!.outcome).isEqualTo(TaskOutcome.FAILED)
  }

  @Test
  fun `MINOR 증가에서 기준선 벡터가 실패해도 보고만 하고 성공한다`() {
    val f = fixture("0.1.0")
    baselineVectors(f, "FAIL")

    val result = f.run("check")

    assertThat(result.task(":baselineVectorTest")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
    val report = File(dir, "build/test-results/baselineVectorTest").listFiles()!!.single { it.extension == "xml" }.readText()
    assertThat(report).contains("<failure")
  }
}
