import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files

class JavaCommentScannerTest {
  @Test
  fun `finds line block javadoc and unicode escaped comments with source lines`() {
    val comments = JavaCommentScanner.find(
      listOf(
        "class Sample {",
        "  // line",
        "  /* block",
        "   * tail */",
        "  /** doc */",
        "  \\u002f\\u002f unicode",
        "  \\u005c\\u002f\\u002f unicode after escaped backslash",
        "}",
      ).joinToString("\n"),
    )

    assertThat(comments.map { it.kind to it.line })
      .containsExactly(
        JavaCommentScanner.Kind.LINE to 2,
        JavaCommentScanner.Kind.BLOCK to 3,
        JavaCommentScanner.Kind.JAVADOC to 5,
        JavaCommentScanner.Kind.LINE to 6,
        JavaCommentScanner.Kind.LINE to 7,
      )
  }

  @Test
  fun `ignores comment shaped strings chars and text blocks`() {
    val comments = JavaCommentScanner.find(
      listOf(
        "class Sample {",
        "  String url = \"https://host/*not a comment*/\";",
        "  char slash = '/';",
        "  String text = \"\"\"",
        "    // not a comment",
        "    /* still text */",
        "    \"\"\";",
        "  // actual",
        "}",
      ).joinToString("\n"),
    )

    assertThat(comments.map { it.line }).containsExactly(8)
  }

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
    fixture.writeAtRepoRoot("src/Sample.java", "class Sample { // line\n}\n")

    val result = fixture.fail("checkJavaComments")

    assertThat(result.output)
      .contains("금지된 Java 주석 1개")
      .contains("src/Sample.java:1: line comment")
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
    fixture.writeAtRepoRoot("generated/Sample.java", "class Sample { // generated\n}\n")
    fixture.writeAtRepoRoot("vendor/Sample.java", "class Sample { // vendor\n}\n")
    fixture.writeAtRepoRoot("third-party/Sample.java", "class Sample { // external\n}\n")
    fixture.writeAtRepoRoot("third_party/Sample.java", "class Sample { // external\n}\n")

    val result = fixture.run("checkJavaComments")

    assertThat(result.output).contains("BUILD SUCCESSFUL")
  }
}
