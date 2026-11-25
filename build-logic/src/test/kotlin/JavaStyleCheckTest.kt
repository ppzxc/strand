import org.gradle.testkit.runner.TaskOutcome
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files

class JavaStyleCheckTest {
  @Test
  fun `checkJavaComments rejects a comment and reports its file and line`() {
    val fixture = BuildFixture(
      Files.createTempDirectory("java-comments").toFile(),
      "java",
      usesCompatExtension = false,
      gitRepository = false,
    )
    fixture.writeAtRepoRoot(
      "build.gradle.kts",
      """
      plugins {
        id("java")
        id("strand.java-comments")
      }
      """.trimIndent() + "\n",
    )
    fixture.writeAtRepoRoot("docs/quality/java-style-baseline.txt", "")
    fixture.writeAtRepoRoot("src/Sample.java", "class Sample { // line\n}\n")

    val result = fixture.fail("check")

    assertThat(result.output)
      .contains("Java 스타일 위반 1개")
      .contains("AllowedJavaComments")
      .contains("src/Sample.java:1")
  }

  @Test
  fun `checkJavaComments allows license and public API javadoc`() {
    val fixture = BuildFixture(
      Files.createTempDirectory("java-comments").toFile(),
      "java",
      usesCompatExtension = false,
      gitRepository = false,
      configurationCache = false,
    )
    fixture.writeAtRepoRoot(
      "build.gradle.kts",
      """
      plugins {
        id("java")
        id("strand.java-comments")
      }
      """.trimIndent() + "\n",
    )
    fixture.writeAtRepoRoot("docs/quality/java-style-baseline.txt", "")
    fixture.writeAtRepoRoot(
      "src/Sample.java",
      """
      /** Copyright 2026 strand authors. */
      public class Sample {
        /** Returns sample value. */
        public String value() { return "sample"; }
      }
      """.trimIndent() + "\n",
    )

    val result = fixture.run("checkJavaComments")

    assertThat(result.task(":checkJavaComments")!!.outcome).isEqualTo(TaskOutcome.SUCCESS)
  }

  @Test
  fun `checkJavaComments rejects public API without javadoc`() {
    val fixture = BuildFixture(
      Files.createTempDirectory("java-comments").toFile(),
      "java",
      usesCompatExtension = false,
      gitRepository = false,
    )
    fixture.writeAtRepoRoot(
      "build.gradle.kts",
      """
      plugins {
        id("java")
        id("strand.java-comments")
      }
      """.trimIndent() + "\n",
    )
    fixture.writeAtRepoRoot("docs/quality/java-style-baseline.txt", "")
    fixture.writeAtRepoRoot(
      "strand-api/src/main/java/p/Sample.java",
      "package p;\npublic class Sample { public String value() { return \"sample\"; } }\n",
    )

    val result = fixture.fail("check")

    assertThat(result.output)
      .contains("PublicApiJavadoc")
      .contains("strand-api/src/main/java/p/Sample.java")
  }

  @Test
  fun `checkJavaComments rejects non JSpecify nullness imports`() {
    val fixture = BuildFixture(
      Files.createTempDirectory("nullness-import").toFile(),
      "java",
      usesCompatExtension = false,
      gitRepository = false,
    )
    fixture.writeAtRepoRoot(
      "build.gradle.kts",
      """
      plugins {
        id("java")
        id("strand.java-comments")
      }
      """.trimIndent() + "\n",
    )
    fixture.writeAtRepoRoot("docs/quality/java-style-baseline.txt", "")
    fixture.writeAtRepoRoot(
      "src/Sample.java",
      "import org.jetbrains.annotations.Nullable;\nclass Sample {}\n",
    )

    val result = fixture.fail("check")

    assertThat(result.output)
      .contains("Java 스타일 위반 1개")
      .contains("NUL-002")
      .contains("src/Sample.java:1")
  }

  @Test
  fun `Spotless check rejects malformed Java formatting`() {
    val fixture = BuildFixture(
      Files.createTempDirectory("spotless-style").toFile(),
      "java",
      usesCompatExtension = false,
      gitRepository = false,
      configurationCache = false,
    )
    fixture.writeAtRepoRoot(
      "build.gradle.kts",
      """
      plugins {
        id("java")
        id("com.diffplug.spotless")
      }
      repositories { mavenCentral() }
      spotless {
        java { palantirJavaFormat("2.103.0") }
      }
      """.trimIndent() + "\n",
    )
    fixture.source("Sample.java", "class Sample{String value(){return \"x\";}}\n")

    val result = fixture.fail("spotlessCheck")

    assertThat(result.output).contains("spotlessJavaCheck").contains("Sample.java")
  }

  @Test
  fun `checkJavaComments ignores generated and third party sources`() {
    val fixture = BuildFixture(
      Files.createTempDirectory("java-comments").toFile(),
      "java",
      usesCompatExtension = false,
      gitRepository = false,
    )
    fixture.writeAtRepoRoot(
      "build.gradle.kts",
      """
      plugins {
        id("java")
        id("strand.java-comments")
      }
      """.trimIndent() + "\n",
    )
    fixture.writeAtRepoRoot("docs/quality/java-style-baseline.txt", "")
    fixture.writeAtRepoRoot("generated/Sample.java", "class Sample { // generated\n}\n")
    fixture.writeAtRepoRoot("vendor/Sample.java", "class Sample { // vendor\n}\n")
    fixture.writeAtRepoRoot("third-party/Sample.java", "class Sample { // external\n}\n")
    fixture.writeAtRepoRoot("third_party/Sample.java", "class Sample { // external\n}\n")

    val result = fixture.run("checkJavaComments")

    assertThat(result.output).contains("BUILD SUCCESSFUL")
  }
}
