import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.FileSystemOperations
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.UntrackedTask
import org.gradle.process.ExecOperations
import java.io.ByteArrayOutputStream
import javax.inject.Inject

/**
 * 기준선 태그의 벡터 디렉터리를 git에서 [outputDir]/<vectorsPath>로 꺼낸다. 기준선이 없으면 아무것도 하지 않는다.
 * 외부 tar 없이 `git ls-tree`와 `git show`만 쓴다.
 */
@UntrackedTask(because = "git 태그 내용에 의존하고 태그는 입력 파일이 아니다")
abstract class ExtractBaselineVectors : DefaultTask() {
  @get:Input
  @get:Optional
  abstract val baseline: Property<String>

  @get:Input
  abstract val vectorsPath: Property<String>

  @get:Internal
  abstract val gitDir: DirectoryProperty

  @get:OutputDirectory
  abstract val outputDir: DirectoryProperty

  @get:Inject
  abstract val exec: ExecOperations

  @get:Inject
  abstract val fs: FileSystemOperations

  @TaskAction
  fun extract() {
    fs.delete { delete(outputDir) }
    val out = outputDir.get().asFile
    out.mkdirs()
    if (!baseline.isPresent) return
    val ref = "refs/tags/${baseline.get()}"
    val listing = ByteArrayOutputStream()
    git(listing, "ls-tree", "-r", "--name-only", "-z", ref, "--", vectorsPath.get())
    val files = listing.toString(Charsets.UTF_8).split('\u0000').filter { it.isNotEmpty() }
    if (files.isEmpty()) {
      throw GradleException("기준선 태그 ${baseline.get()}에 ${vectorsPath.get()}가 없다")
    }
    for (path in files) {
      val target = out.resolve(path)
      target.parentFile.mkdirs()
      target.outputStream().use { git(it, "show", "$ref:$path") }
    }
  }

  private fun git(stdout: java.io.OutputStream, vararg args: String) {
    val err = ByteArrayOutputStream()
    val result = exec.exec {
      commandLine("git", *args)
      workingDir = gitDir.get().asFile
      standardOutput = stdout
      errorOutput = err
      isIgnoreExitValue = true
    }
    if (result.exitValue != 0) {
      throw GradleException("git ${args.joinToString(" ")} 실패: ${err.toString(Charsets.UTF_8).trim()}")
    }
  }
}
