package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Event;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 한 연결에서 요청이 많이 오가는 경우를 실제 TCP 위에서 확인한다. */
class RequestVolumeIntegrationTest {

    private static final int TOTAL_REQUESTS = 1000;
    /** 동시 스트림 한도(기본 100) 안에서 이어 보내도록 아직 응답받지 못한 요청을 이 수 이하로 유지한다. */
    private static final int MAX_OUTSTANDING = 50;
    /** 요청 {@code i}는 {@code type}을 {@code APPLICATION_TYPE + i % 8}로 보내고, 서버는 {@code i % 8}을 {@code status}로 응답한다. */
    private static final int STATUS_CYCLE = 8;

    /**
     * 요청을 이어 보내는 동안 스트림 번호가 어긋나거나 응답이 다른 요청에 매핑되거나 유실되면 결과가 모자라거나 번호별 {@code status}가 달라진다. 클라이언트
     * 번호 공간은 1, 3, 5...로 늘어나므로(규격 4.2절) {@code i}번째 요청은 {@code stream_id} {@code 2i + 1}을 받는다. 요청 {@value #TOTAL_REQUESTS}개
     * 모두에 대해 그 번호의 결과 {@code status}가 {@code i % 8}이어야 한다. 응답 도착 순서는 규격이 정하지 않아 번호별 매핑으로 비교한다.
     */
    @Test
    @DisplayName("요청 1,000개를 이어 보내도 각 stream_id의 응답이 자기 요청에 매핑된다 (규격 4.2)")
    void sequentialRequestsKeepStreamNumbersAndResponses() throws Exception {
        try (TestServer server = TestServer.startResponding(type -> (type - APPLICATION_TYPE));
                TestClient client = TestClient.connect(server.port())) {
            Map<Long, Integer> statusByStream = new HashMap<>();
            int sent = 0;
            int received = 0;
            while (received < TOTAL_REQUESTS) {
                while (sent < TOTAL_REQUESTS && sent - received < MAX_OUTSTANDING) {
                    client.request(APPLICATION_TYPE + sent % STATUS_CYCLE, Fixtures.payload("request-" + sent));
                    sent++;
                }
                Event result = client.awaitEvent(Event.Kind.RESULT);
                statusByStream.put(result.streamId(), result.status());
                received++;
            }

            Map<Long, Integer> expected = new HashMap<>();
            for (int i = 0; i < TOTAL_REQUESTS; i++) {
                expected.put(2L * i + 1, i % STATUS_CYCLE);
            }
            assertThat(statusByStream).isEqualTo(expected);
        }
    }
}
