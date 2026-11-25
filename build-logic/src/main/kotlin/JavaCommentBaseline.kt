import java.io.File
import java.security.MessageDigest

object JavaCommentBaseline {
  fun newComments(files: Iterable<File>, root: File, baseline: List<String>): List<String> {
    val allowed = baseline.groupingBy { it }.eachCount().toMutableMap()
    return files.sortedBy { it.relativeTo(root).invariantSeparatorsPath }.flatMap { file ->
      JavaCommentScanner.find(file.readText()).mapNotNull { comment ->
        val key = key(file.relativeTo(root).invariantSeparatorsPath, comment.text)
        val count = allowed[key] ?: 0
        if (count > 0) {
          allowed[key] = count - 1
          null
        } else {
          "${file.relativeTo(root).invariantSeparatorsPath}:${comment.line}: new ${comment.kind.name.lowercase()} comment"
        }
      }
    }
  }

  fun contents(files: Iterable<File>, root: File): String =
    files.flatMap { file ->
      val path = file.relativeTo(root).invariantSeparatorsPath
      JavaCommentScanner.find(file.readText()).map { key(path, it.text) }
    }.sorted().joinToString("\n", postfix = "\n")

  private fun key(path: String, comment: String): String = "$path\t${comment.sha256()}"

  private fun String.sha256(): String =
    MessageDigest.getInstance("SHA-256").digest(toByteArray()).joinToString("") { "%02x".format(it.toInt() and 0xff) }
}
