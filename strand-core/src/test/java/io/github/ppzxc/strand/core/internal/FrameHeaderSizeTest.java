package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FrameHeaderSizeTest {

    @Test
    void thePublishedHeaderSizeIsTheSizeCoreEncodes() {
        assertThat(Endpoint.FRAME_HEADER_SIZE).isEqualTo(Frames.HEADER_SIZE);
    }

    @Test
    void theHeaderSizeIsTwentyBytesAsTheSpecificationFixes() {
        assertThat(Endpoint.FRAME_HEADER_SIZE).isEqualTo(20);
    }
}
