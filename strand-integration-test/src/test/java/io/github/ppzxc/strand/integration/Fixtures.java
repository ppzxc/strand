package io.github.ppzxc.strand.integration;

import io.github.ppzxc.strand.api.Result;
import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** 통합 테스트가 함께 쓰는 값. */
final class Fixtures {

    /** 이벤트나 종료를 기다리는 상한. 넘기면 실패시켜 테스트가 멈추지 않게 한다. */
    static final long TIMEOUT_MS = 5000L;

    /** 규격 4.1절: 애플리케이션 type은 256 이상이다. */
    static final int APPLICATION_TYPE = 256;

    /** 규격 2.3절: PING의 type. */
    static final int PING_TYPE = 2;

    /** 규격 2.3절: CLOSE의 type. */
    static final int CLOSE_TYPE = 3;

    /** 규격 2.5.1절 status {@code OK}. */
    static final int STATUS_OK = 0;

    /** 규격 2.5.1절 status {@code UNAVAILABLE}. */
    static final int STATUS_UNAVAILABLE = 14;

    /** 규격 2.1절, 3.5절: CLOSE 프레임은 헤더 20바이트에 {@code error_code} 4바이트와 {@code last_stream_id} 8바이트가 붙은 32바이트다(확장 영역과 {@code reason}이 없을 때). */
    static final int CLOSE_FRAME_BYTES = 32;

    private Fixtures() {}

    static byte[] payload(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /** 보존된 결과가 확정되길 상한 안에서 기다려 꺼낸다. */
    static <T> T await(Result<T> result) throws Exception {
        return result.toCompletionStage(Peer.HANDLERS).toCompletableFuture().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * 통로가 읽은 프레임 가운데 상대가 낸 애플리케이션 요청의 {@code stream_id}별 type. 제품 API는 받은 요청의 스트림 번호를 드러내지 않아 통로로 본다. 응답 프레임도 type이 256
     * 이상이고 {@code stream_id}가 상대 엔드포인트의 번호 공간이므로(규격 4.2절), 읽는 쪽이 아니라 보낸 쪽의 번호 공간(클라이언트는 홀수, 서버는 짝수)인 프레임만 요청으로 센다.
     *
     * @param sentByClient 프레임을 보낸 쪽이 클라이언트인가(서버 통로가 읽은 프레임이면 true)
     */
    static Map<Long, Integer> requestTypeByStream(List<ControlledPassage.Frame> frames, boolean sentByClient) {
        Map<Long, Integer> typeByStream = new HashMap<>();
        for (ControlledPassage.Frame frame : frames) {
            boolean oddStream = (frame.streamId() & 1) == 1;
            if (frame.type() >= APPLICATION_TYPE && oddStream == sentByClient) {
                typeByStream.put(frame.streamId(), frame.type());
            }
        }
        return typeByStream;
    }
}
