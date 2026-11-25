/** Finds Java comments after Unicode escape translation while retaining the original source line and spelling. */
object JavaCommentScanner {
  enum class Kind { LINE, BLOCK, JAVADOC }

  data class Comment(val kind: Kind, val line: Int, val text: String)

  fun find(source: String): List<Comment> {
    val translated = translateUnicodeEscapes(source)
    val result = mutableListOf<Comment>()
    var index = 0
    while (index < translated.text.length) {
      when {
        translated.text.startsWith("//", index) -> {
          val start = index
          val rawStart = translated.starts[start]
          index += 2
          while (index < translated.text.length && translated.text[index] != '\n' && translated.text[index] != '\r') index++
          result += comment(source, Kind.LINE, translated, start, index, rawStart)
        }
        translated.text.startsWith("/*", index) -> {
          val start = index
          val rawStart = translated.starts[start]
          val kind = if (translated.text.startsWith("/**", index)) Kind.JAVADOC else Kind.BLOCK
          index += 2
          while (index < translated.text.length && !translated.text.startsWith("*/", index)) index++
          if (index < translated.text.length) index += 2
          result += comment(source, kind, translated, start, index, rawStart)
        }
        translated.text.startsWith("\"\"\"", index) && isTextBlockStart(translated.text, index) -> {
          index += 3
          while (index < translated.text.length) {
            if (translated.text.startsWith("\"\"\"", index) && !isEscaped(translated.text, index)) {
              index += 3
              break
            }
            index++
          }
        }
        translated.text[index] == '"' || translated.text[index] == '\'' -> {
          val quote = translated.text[index++]
          while (index < translated.text.length) {
            when {
              translated.text[index] == '\\' -> index = (index + 2).coerceAtMost(translated.text.length)
              translated.text[index++] == quote -> break
            }
          }
        }
        else -> index++
      }
    }
    return result
  }

  private fun isTextBlockStart(text: String, start: Int): Boolean {
    var index = start + 3
    while (index < text.length && (text[index] == ' ' || text[index] == '\t' || text[index] == '\u000c')) index++
    return index < text.length && (text[index] == '\n' || text[index] == '\r')
  }

  private fun isEscaped(text: String, index: Int): Boolean {
    var slashes = 0
    var cursor = index - 1
    while (cursor >= 0 && text[cursor--] == '\\') slashes++
    return slashes % 2 != 0
  }

  private fun comment(source: String, kind: Kind, translated: Translation, start: Int, end: Int, rawStart: Int): Comment =
    Comment(kind, translated.lines[start], source.substring(rawStart, translated.ends[end - 1]))

  private data class Translation(val text: String, val starts: IntArray, val ends: IntArray, val lines: IntArray)

  private fun translateUnicodeEscapes(source: String): Translation {
    val text = StringBuilder()
    val starts = mutableListOf<Int>()
    val ends = mutableListOf<Int>()
    val lines = mutableListOf<Int>()
    var index = 0
    var line = 1
    var backslashes = 0
    var previousCharacterWasUnicodeEscape = false
    while (index < source.length) {
      val start = index
      var char = source[index]
      var end = index + 1
      if (
        char == '\\' && (previousCharacterWasUnicodeEscape || backslashes % 2 == 0) &&
        index + 1 < source.length && source[index + 1] == 'u'
      ) {
        var digits = index + 1
        while (digits < source.length && source[digits] == 'u') digits++
        if (digits + 4 <= source.length && source.substring(digits, digits + 4).all { it.isHexDigit() }) {
          char = source.substring(digits, digits + 4).toInt(16).toChar()
          end = digits + 4
        }
      }
      text.append(char)
      starts += start
      ends += end
      lines += line
      if (source.substring(start, end).contains('\n')) line += source.substring(start, end).count { it == '\n' }
      backslashes = if (char == '\\') backslashes + 1 else 0
      previousCharacterWasUnicodeEscape = end > start
      index = end
    }
    return Translation(text.toString(), starts.toIntArray(), ends.toIntArray(), lines.toIntArray())
  }

  private fun Char.isHexDigit() = this in '0'..'9' || this in 'a'..'f' || this in 'A'..'F'
}
