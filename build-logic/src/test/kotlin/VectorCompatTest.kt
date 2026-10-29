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
        if (dir == null) fail("strand.conformance.vectors.dir가 전달되지 않았다");
        try (Stream<Path> stream = Files.list(Paths.get(dir))) {
          java.util.List<Path> files = stream.collect(Collectors.toList());
          if (files.isEmpty()) fail("벡터가 하나도 읽히지 않았다: " + dir);
          for (Path p : files) {
            if (new String(Files.readAllBytes(p)).contains("FAIL")) fail("rejected " + p.getFileName());
          }
        }
      }
    }
  """.trimIndent()

  private fun fixture(version: String): BuildFixture =
    BuildFixture(dir, "strand.vector-compat-conventions").also {
      it.buildScript(version, fixtureBuildExtra)
      it.write("src/test/java/VectorConformanceTest.java", runnerTest)
    }

  private val fixtureBuildExtra =
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
        """.trimIndent()

  private fun rejectedReport(): String =
    File(dir, "build/test-results/baselineVectorTest").listFiles()!!.single { it.extension == "xml" }.readText()

  private fun baselineVectors(f: BuildFixture, content: String, working: String = "ok") {
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
    assertThat(rejectedReport()).contains("rejected v.json")
  }

  @Test
  fun `MINOR 증가에서 기준선 벡터가 실패해도 보고만 하고 성공한다`() {
    val f = fixture("0.1.0")
    baselineVectors(f, "FAIL")

    val result = f.run("check")

    assertThat(result.task(":baselineVectorTest")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
    assertThat(rejectedReport()).contains("<failure").contains("rejected v.json")
  }

  @Test
  fun `MINOR로 실행한 뒤 PATCH로 올리면 UP-TO-DATE로 실패가 사라지지 않는다`() {
    val f = fixture("0.1.0")
    baselineVectors(f, "FAIL")
    f.run("baselineVectorTest")

    f.setVersion("0.0.2")
    val result = f.fail("baselineVectorTest")

    assertThat(result.task(":baselineVectorTest")!!.outcome).isEqualTo(TaskOutcome.FAILED)
  }

  @Test
  fun `기준선이 바뀌어 벡터 내용이 달라지면 UP-TO-DATE로 건너뛰지 않는다`() {
    val f = fixture("0.0.3")
    baselineVectors(f, "ok")
    f.run("baselineVectorTest")
    f.write("conformance/vectors/v.json", "FAIL")
    f.commitAndTag("0.0.2")
    f.write("conformance/vectors/v.json", "ok")

    val result = f.fail("baselineVectorTest")

    assertThat(result.task(":baselineVectorTest")!!.outcome).isEqualTo(TaskOutcome.FAILED)
  }

  @Test
  fun `입력이 같으면 configuration cache를 재사용하고 테스트는 UP-TO-DATE다`() {
    val f = fixture("0.0.2")
    baselineVectors(f, "ok")
    f.run("baselineVectorTest")

    val second = f.run("baselineVectorTest")

    assertThat(second.output).contains("Reusing configuration cache")
    assertThat(second.task(":baselineVectorTest")!!.outcome).isEqualTo(TaskOutcome.UP_TO_DATE)
  }

  @Test
  fun `테스트 필터가 적용되어 벡터 러너 외의 테스트는 돌지 않는다`() {
    val f = fixture("0.0.2")
    f.write("src/test/java/OtherTest.java", "class OtherTest { @org.junit.jupiter.api.Test void other() { org.junit.jupiter.api.Assertions.fail(\"필터가 무시됐다\"); } }")
    baselineVectors(f, "ok")

    val result = f.run("baselineVectorTest")

    assertThat(result.task(":baselineVectorTest")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
  }

  @Test
  fun `프로젝트가 git 저장소의 하위 디렉터리여도 기준선 벡터를 루트 기준 경로로 꺼낸다`() {
    val f = BuildFixture(dir, "strand.vector-compat-conventions", projectDir = File(dir, "sub").also { it.mkdirs() })
    f.buildScript("0.0.2", fixtureBuildExtra)
    f.write("src/test/java/VectorConformanceTest.java", runnerTest)
    f.writeAtRepoRoot("conformance/vectors/v.json", "ok")
    f.commitAndTag("0.0.1")
    f.writeAtRepoRoot("conformance/vectors/v.json", "FAIL") // 작업 트리 벡터는 실패지만 기준선은 통과

    val result = f.run("baselineVectorTest")

    assertThat(result.task(":baselineVectorTest")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
  }
}
