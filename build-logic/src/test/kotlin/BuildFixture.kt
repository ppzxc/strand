import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import java.io.File
import java.nio.file.Files
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import javax.tools.ToolProvider

/**
 * 임시 디렉터리에 git 저장소, 태그, 파일 기반 Maven 저장소(기준선 jar), 벡터를 만들고 GradleRunner로 빌드한다.
 * 태그는 이 임시 저장소에만 만든다.
 */
class BuildFixture(
  val dir: File,
  private val plugin: String,
  /** `strandCompat { }` 블록(기준선 저장소 지정)을 쓰는 호환성 플러그인인가. */
  private val usesCompatExtension: Boolean = true,
  /** false면 git 저장소를 만들지 않는다. */
  gitRepository: Boolean = true,
) {
  private var extraScript = ""
  val repo = File(dir, "baseline-repo").also { it.mkdirs() }

  init {
    if (gitRepository) git("init", "-q")
    write(".gitignore", "build/\n.gradle/\nbaseline-repo/\n")
    // 실제 저장소의 카탈로그를 그대로 쓴다. build-logic 테스트의 작업 디렉터리는 build-logic이다.
    val catalog = File("../gradle/libs.versions.toml").absoluteFile.invariantSeparatorsPath
    write(
      "settings.gradle.kts",
      """
      rootProject.name = "proj"
      dependencyResolutionManagement { versionCatalogs { create("libs") { from(files("$catalog")) } } }
      """.trimIndent() + "\n",
    )
    write("gradle.properties", "group=g\n")
  }

  /** 빌드 스크립트를 쓴다. [extra]는 이후 [setVersion]에도 유지된다. */
  fun buildScript(version: String, extra: String = "") {
    extraScript = extra
    writeBuildScript(version)
  }

  fun setVersion(version: String) = writeBuildScript(version)

  private fun writeBuildScript(version: String) {
    write(
      "build.gradle.kts",
      """
      plugins { id("$plugin") }
      version = "$version"
      // 프로젝트 이름과 산출물 이름을 다르게 둬서(실제 저장소의 core/strand-core처럼) 기준선이 자기 자신으로 해석되지 않게 한다.
      base.archivesName = "lib"
      repositories { mavenCentral() }
      ${if (usesCompatExtension) "strandCompat { baselineRepository = \"${repo.toURI()}\" }" else ""}
      $extraScript
      """.trimIndent() + "\n",
    )
  }

  fun write(path: String, text: String) {
    val file = File(dir, path)
    file.parentFile.mkdirs()
    file.writeText(text)
  }

  fun source(path: String, text: String) = write("src/main/java/$path", text)

  fun git(vararg args: String): String {
    val process = ProcessBuilder(listOf("git", "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false") + args)
      .directory(dir).redirectErrorStream(true).start()
    val out = process.inputStream.bufferedReader().readText()
    check(process.waitFor() == 0) { "git ${args.toList()} 실패: $out" }
    return out
  }

  /** 지금 작업 트리를 커밋하고 태그를 단다. */
  fun commitAndTag(tag: String) {
    git("add", "-A")
    git("commit", "-q", "--allow-empty", "-m", "snapshot $tag")
    git("tag", tag)
  }

  /** 기준선 jar를 [version]으로 file repo에 직접 만든다. [sources]는 경로(`p/Api.java`) to 소스. */
  fun publishBaselineJar(version: String, sources: Map<String, String>) {
    val classes = Files.createTempDirectory(dir.toPath(), "cls").toFile()
    val compiler = ToolProvider.getSystemJavaCompiler()
    val srcRoot = Files.createTempDirectory(dir.toPath(), "src").toFile()
    val files = sources.map { (path, text) -> File(srcRoot, path).also { it.parentFile.mkdirs(); it.writeText(text) } }
    val code = compiler.run(null, null, null, "-d", classes.path, *files.map { it.path }.toTypedArray())
    check(code == 0) { "기준선 소스 컴파일 실패" }
    val moduleDir = File(repo, "g/lib/$version").also { it.mkdirs() }
    JarOutputStream(File(moduleDir, "lib-$version.jar").outputStream()).use { jar ->
      classes.walkTopDown().filter { it.isFile }.forEach {
        jar.putNextEntry(JarEntry(it.relativeTo(classes).invariantSeparatorsPath))
        jar.write(it.readBytes())
        jar.closeEntry()
      }
    }
    File(moduleDir, "lib-$version.pom").writeText(
      "<project><modelVersion>4.0.0</modelVersion><groupId>g</groupId><artifactId>lib</artifactId><version>$version</version></project>",
    )
    classes.deleteRecursively()
    srcRoot.deleteRecursively()
  }

  private fun runner(vararg args: String) = GradleRunner.create()
    .withProjectDir(dir)
    .withPluginClasspath()
    .withArguments(*args, "--configuration-cache", "--stacktrace")

  fun run(vararg args: String): BuildResult = runner(*args).build()

  fun fail(vararg args: String): BuildResult = runner(*args).buildAndFail()
}
