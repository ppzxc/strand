package io.github.ppzxc.strand.load;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class StderrCaptureTest {
    @Test
    void countsLeaksBeyondRetainedLogAndLeakExamples() throws Exception {
        StringBuilder input = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            input.append("LEAK: message ").append(i).append('\n');
        }
        input.append("tail without newline");

        Orchestrator.StderrCapture capture = new Orchestrator.StderrCapture(
                new ByteArrayInputStream(input.toString().getBytes(StandardCharsets.UTF_8)));
        capture.drain();

        assertThat(capture.leakCount()).isEqualTo(100);
        assertThat(capture.leakLines()).hasSizeLessThanOrEqualTo(20);
        assertThat(capture.text()).contains("tail without newline").hasSizeLessThanOrEqualTo(16_384);
    }

    @Test
    void truncatesAnUnterminatedOversizedLineWhileDetectingLeakMarker() throws Exception {
        String input = "x".repeat(50_000) + "LEAK: oversized";
        Orchestrator.StderrCapture capture =
                new Orchestrator.StderrCapture(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
        capture.drain();

        assertThat(capture.leakCount()).isEqualTo(1);
        assertThat(capture.leakLines())
                .singleElement()
                .asString()
                .contains("LEAK:")
                .hasSizeLessThanOrEqualTo(2_048);
        assertThat(capture.text()).hasSizeLessThanOrEqualTo(16_384);
    }
}
