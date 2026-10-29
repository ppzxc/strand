import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/** PATCH만 증가했는지 판정하는 순수 함수의 단위 테스트. */
class CompatBaselineTest {
  private fun v(text: String) = SemVer.parseTag(text)!!

  @Test
  fun `patch만 증가하면 PATCH다`() {
    assertThat(isPatchOnlyIncrease(v("1.2.4"), v("1.2.3"))).isTrue()
    assertThat(isPatchOnlyIncrease(v("0.0.10"), v("0.0.1"))).isTrue()
  }

  @Test
  fun `minor가 증가하면 PATCH가 아니다 patch가 줄어도 마찬가지다`() {
    assertThat(isPatchOnlyIncrease(v("1.3.0"), v("1.2.3"))).isFalse()
    assertThat(isPatchOnlyIncrease(v("1.3.1"), v("1.2.9"))).isFalse()
  }

  @Test
  fun `major가 증가하면 PATCH가 아니다`() {
    assertThat(isPatchOnlyIncrease(v("2.0.0"), v("1.9.9"))).isFalse()
    assertThat(isPatchOnlyIncrease(v("2.2.4"), v("1.2.3"))).isFalse()
  }

  @Test
  fun `같거나 감소하면 PATCH가 아니다`() {
    assertThat(isPatchOnlyIncrease(v("1.2.3"), v("1.2.3"))).isFalse()
    assertThat(isPatchOnlyIncrease(v("1.2.2"), v("1.2.3"))).isFalse()
    assertThat(isPatchOnlyIncrease(v("1.1.9"), v("1.2.3"))).isFalse()
  }

  @Test
  fun `범위를 넘는 숫자는 태그로 인정하지 않는다`() {
    assertThat(SemVer.parseTag("99999999999.0.0")).isNull()
    assertThat(SemVer.parseVersion("0.0.99999999999")).isNull()
  }
}
