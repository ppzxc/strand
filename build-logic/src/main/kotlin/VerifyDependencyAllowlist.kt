import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.provider.SetProperty
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

/**
 * 배포 라이브러리의 의존성 0 규칙을 검사한다.
 *
 * - runtimeClasspath: 해석된 컴포넌트가 하나도 없어야 한다.
 * - compileClasspath: [allowedCompile]에 있는 좌표(`group:name:version`)만 허용한다.
 *
 * 입력은 해석 결과 문자열 집합이라 configuration cache와 호환된다.
 */
@CacheableTask
abstract class VerifyDependencyAllowlist : DefaultTask() {
  @get:Input
  abstract val modulePath: Property<String>

  @get:Input
  abstract val runtimeComponents: SetProperty<String>

  @get:Input
  abstract val compileComponents: SetProperty<String>

  @get:Input
  abstract val allowedCompile: SetProperty<String>

  @get:OutputFile
  abstract val marker: RegularFileProperty

  @TaskAction
  fun verify() {
    val problems = mutableListOf<String>()
    val runtime = runtimeComponents.get()
    if (runtime.isNotEmpty()) {
      problems += "runtimeClasspath는 비어 있어야 한다. 발견: ${runtime.sorted()}"
    }
    val unexpected = compileComponents.get() - allowedCompile.get()
    if (unexpected.isNotEmpty()) {
      problems += "compileClasspath에는 ${allowedCompile.get().sorted()}만 허용된다. 허용 외: ${unexpected.sorted()}"
    }
    if (problems.isNotEmpty()) {
      throw GradleException("${modulePath.get()} 의존성 0 검사 실패\n" + problems.joinToString("\n") { "  - $it" })
    }
    marker.get().asFile.writeText("ok\n")
  }
}
