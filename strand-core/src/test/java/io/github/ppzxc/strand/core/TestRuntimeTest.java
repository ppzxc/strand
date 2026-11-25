package io.github.ppzxc.strand.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

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

        String normalized = actual.startsWith("1.") ? actual.substring(2) : actual;
        assertThat(normalized).isEqualTo(expected);
    }
}
