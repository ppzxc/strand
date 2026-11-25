import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PolicyExceptionBaselineTest {
  @Test
  fun removingAnOldFindingDoesNotOffsetANewOne() {
    val current = listOf("strand-core/src/Old.java:10:missing rule=OLD", "strand-core/src/New.java:20:missing rule=NEW")
    val baseline = listOf("strand-core/src/Old.java:10:missing rule=OLD")

    assertThat(unapprovedPolicyFindings(current, baseline))
      .containsExactly("strand-core/src/New.java:20:missing rule=NEW")
  }

  @Test
  fun duplicateSameLineFindingsExceedBaselineCount() {
    val finding = "strand-core/src/Sample.java:10:missing rule=PMD.Rule"

    assertThat(unapprovedPolicyFindings(listOf(finding, finding), listOf(finding)))
      .containsExactly(finding)
  }
}
