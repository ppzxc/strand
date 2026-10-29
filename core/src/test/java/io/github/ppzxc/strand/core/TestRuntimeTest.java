package io.github.ppzxc.strand.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * CI 런타임 매트릭스의 증거. 테스트가 실제로 어느 JDK에서 도는지 로그에 남기고,
 * 빌드가 기대 런타임(-PtestJavaVersion)을 넘겨 주면 그 버전에서 돌았는지 검사한다.
 */
class TestRuntimeTest {

    @Test
    void runsOnTheRequestedRuntime() {
        System.out.println("TEST-RUNTIME java.version=" + System.getProperty("java.version")
                + " java.specification.version=" + System.getProperty("java.specification.version")
                + " java.home=" + System.getProperty("java.home"));

        String expected = System.getProperty("strand.expectedJavaVersion");
        if (expected == null) {
            return;
        }
        String actual = System.getProperty("java.specification.version");
        // JDK 8은 "1.8", 9 이상은 "9", "11" 형태다.
        String normalized = actual.startsWith("1.") ? actual.substring(2) : actual;
        assertThat(normalized).isEqualTo(expected); if (expected.equals("8")) { throw new AssertionError("intentional"); }
    }
}
