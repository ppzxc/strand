import org.gradle.api.DefaultTask
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

/** 기준선 태그의 벡터 디렉터리를 git에서 [outputDir]/<vectorsPath>로 꺼낸다. 기준선이 없으면 아무것도 하지 않는다. */
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
    outputDir.get().asFile.mkdirs()
    if (!baseline.isPresent) return
    val archive = outputDir.get().asFile.resolve("vectors.tar")
    val err = ByteArrayOutputStream()
    val result = exec.exec {
      commandLine("git", "archive", "--format=tar", "-o", archive.absolutePath, baseline.get(), vectorsPath.get())
      workingDir = gitDir.get().asFile
      errorOutput = err
      isIgnoreExitValue = true
    }
    check(result.exitValue == 0) { "기준선 태그 ${baseline.get()}에서 ${vectorsPath.get()}를 꺼내지 못했다: $err" }
    exec.exec {
      commandLine("tar", "-xf", archive.absolutePath, "-C", outputDir.get().asFile.absolutePath)
    }
    archive.delete()
  }
}
