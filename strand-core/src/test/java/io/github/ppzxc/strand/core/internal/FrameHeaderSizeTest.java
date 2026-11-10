package io.github.ppzxc.strand.core.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** 전송 모듈이 쓰는 프레임 헤더 크기는 core가 인코딩하는 헤더 크기와 같은 값이다(규격 2.1절). */
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
