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
  fun `baseline allows registered comments and rejects extra copies`() {
    val root = Files.createTempDirectory("java-comments").toFile()
    val source = root.resolve("Sample.java").apply { writeText("class Sample { // line\n}\n") }
    val baseline = JavaCommentBaseline.contents(listOf(source), root).trim().lines()

    assertThat(JavaCommentBaseline.newComments(listOf(source), root, baseline)).isEmpty()

    source.writeText("class Sample { // line\n  // line\n}\n")
    assertThat(JavaCommentBaseline.newComments(listOf(source), root, baseline))
      .containsExactly("Sample.java:2: new line comment")
  }
}
