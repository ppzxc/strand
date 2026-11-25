import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PolicyExceptionBaselineTest {
  @Test
  fun removingAnOldFindingDoesNotOffsetANewOne() {
    val current = setOf("strand-core/src/Old.java:10", "strand-core/src/New.java:20")
    val baseline = setOf("strand-core/src/Old.java:10")

    assertThat(unapprovedPolicyFindings(current, baseline))
      .containsExactly("strand-core/src/New.java:20")
  }
}
