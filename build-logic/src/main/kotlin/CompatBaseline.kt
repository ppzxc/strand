import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.Property
import org.gradle.api.provider.Provider
import org.gradle.api.provider.ValueSource
import org.gradle.api.provider.ValueSourceParameters
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.UntrackedTask
import org.gradle.process.ExecOperations
import java.io.ByteArrayOutputStream
import javax.inject.Inject

/** 호환성 검사 설정. `strandCompat { }` 블록으로 바꾼다. */
abstract class StrandCompatExtension {
  /** 기준선 jar를 찾을 저장소. `central`(기본), `mavenLocal`, 또는 URL. `-Pstrand.compat.baselineRepository=`로도 정한다. */
  abstract val baselineRepository: Property<String>

  /** 저장소 루트 기준 벡터 디렉터리. 기준선 태그에서 이 경로를 꺼낸다. */
  abstract val vectorsPath: Property<String>

  /** 기준선 벡터 재실행에서 돌릴 테스트(Gradle 테스트 필터 패턴). */
  abstract val vectorTestFilter: Property<String>
}

/** `X.Y.Z` 정확히 한 형태만 받는다. `v1.2.3`이나 `1.2.3-rc1` 같은 태그는 기준선 후보가 아니다. */
data class SemVer(val major: Int, val minor: Int, val patch: Int) : Comparable<SemVer> {
  override fun compareTo(other: SemVer): Int =
    compareValuesBy(this, other, SemVer::major, SemVer::minor, SemVer::patch)

  override fun toString() = "$major.$minor.$patch"

  companion object {
    private val TAG = Regex("""(\d+)\.(\d+)\.(\d+)""")
    private val VERSION_PREFIX = Regex("""(\d+)\.(\d+)\.(\d+).*""")

    /** 태그 이름용. 정확히 X.Y.Z만. */
    fun parseTag(text: String): SemVer? = TAG.matchEntire(text)?.toSemVer()

    /** 프로젝트 `version`용. `0.1.0-SNAPSHOT` 같은 접미어는 무시하고 숫자 부분만 본다. */
    fun parseVersion(text: String): SemVer? = VERSION_PREFIX.matchEntire(text)?.toSemVer()

    // Int 범위를 넘는 숫자는 버전으로 인정하지 않는다.
    private fun MatchResult.toSemVer(): SemVer? {
      val (major, minor, patch) = groupValues.drop(1).map { it.toIntOrNull() ?: return null }
      return SemVer(major, minor, patch)
    }
  }
}

/** [version]보다 작은 X.Y.Z 태그 중 최댓값. 없으면 null. */
fun selectBaseline(version: SemVer, tags: List<String>): SemVer? =
  tags.mapNotNull(SemVer::parseTag).filter { it < version }.maxOrNull()

/** 기준선과 major.minor가 같고 patch만 올랐으면 true. 이때만 호환성 위반이 빌드를 실패시킨다. */
fun isPatchOnlyIncrease(version: SemVer, baseline: SemVer): Boolean =
  version.major == baseline.major && version.minor == baseline.minor && version.patch > baseline.patch

/**
 * 로컬 git 태그에서 기준선 버전 하나를 계산한다. 기준선이 없으면(태그 없음, 더 큰 태그뿐, git 저장소 아님) 값이 없다.
 * 매 빌드 다시 읽히므로 태그가 바뀌면 configuration cache가 무효화된다.
 */
abstract class GitBaselineSource : ValueSource<String, GitBaselineSource.Params> {
  interface Params : ValueSourceParameters {
    val projectVersion: Property<String>
    val gitDir: DirectoryProperty
  }

  @get:Inject
  abstract val exec: ExecOperations

  override fun obtain(): String? {
    val version = SemVer.parseVersion(parameters.projectVersion.get()) ?: return null
    val out = ByteArrayOutputStream()
    val err = ByteArrayOutputStream()
    val result = try {
      exec.exec {
        commandLine("git", "tag", "--list")
        workingDir = parameters.gitDir.get().asFile
        environment("LC_ALL", "C") // 오류 메시지 판별이 로케일에 흔들리지 않게 한다.
        standardOutput = out
        errorOutput = err
        isIgnoreExitValue = true
      }
    } catch (e: Exception) {
      return null // git이 없으면 태그도 없는 것으로 본다.
    }
    if (result.exitValue != 0) {
      if ("not a git repository" in err.toString(Charsets.UTF_8)) return null // 소스 압축본 같은 비 git 빌드
      // 저장소 안에서의 실패(safe.directory, 손상 등)를 기준선 없음으로 삼키면 검사가 조용히 꺼진다.
      throw GradleException("git tag --list 실패(종료 코드 ${result.exitValue}): ${err.toString(Charsets.UTF_8).trim()}")
    }
    return selectBaseline(version, out.toString(Charsets.UTF_8).lines().map { it.trim() })?.toString()
  }
}

/** 기준선 버전 provider. 호환성 검사 태스크가 모두 같은 값을 본다. */
fun Project.compatBaselineVersion(): Provider<String> =
  providers.of(GitBaselineSource::class.java) {
    parameters.projectVersion.set(provider { version.toString() })
    parameters.gitDir.set(rootProject.layout.projectDirectory)
  }

/** 기준선이 있으면 true. 태스크의 onlyIf에 쓴다. */
fun Project.compatHasBaseline(): Provider<Boolean> = compatBaselineVersion().map { true }.orElse(false)

/** PATCH만 올랐으면 true. 기준선이 없으면 값이 없다. */
fun Project.compatPatchOnly(): Provider<Boolean> {
  val projectVersion = provider { version.toString() }
  return compatBaselineVersion().zip(projectVersion) { baseline, current ->
    isPatchOnlyIncrease(SemVer.parseVersion(current)!!, SemVer.parseTag(baseline)!!)
  }
}

/** 기준선을 출력한다. 출력은 "기준선 없음" 또는 "기준선 X.Y.Z" 하나. */
@UntrackedTask(because = "git 태그 상태를 출력만 한다")
abstract class CompatBaselineTask : DefaultTask() {
  @get:Input
  @get:Optional
  abstract val baseline: Property<String>

  @TaskAction
  fun print() {
    logger.lifecycle(if (baseline.isPresent) "기준선 ${baseline.get()}" else "기준선 없음")
  }
}
