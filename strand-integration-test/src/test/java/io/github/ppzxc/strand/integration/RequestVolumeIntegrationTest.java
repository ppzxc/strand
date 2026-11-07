package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Event;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 한 연결에서 요청이 많이 오가는 경우를 실제 TCP 위에서 확인한다. */
class RequestVolumeIntegrationTest {

    private static final int TOTAL_REQUESTS = 1000;
    /** 동시 스트림 한도(기본 100) 안에서 이어 보내도록 아직 응답받지 못한 요청을 이 수 이하로 유지한다. */
    private static final int MAX_OUTSTANDING = 50;

    /**
     * 요청을 이어 보내는 동안 스트림 번호가 어긋나거나 응답이 유실되면 결과가 모자라거나 번호 집합이 달라진다. 클라이언트 번호 공간은 1, 3, 5...로 늘어나므로
     * (규격 4.2절) 요청 {@value #TOTAL_REQUESTS}개의 결과가 모두 {@code OK}이고 {@code stream_id}가 정확히 {@code 1, 3, ..., 1999}여야 한다. 응답
     * 도착 순서는 규격이 정하지 않아 번호 집합으로 비교한다.
     */
    @Test
    @DisplayName("요청 1,000개를 이어 보내도 응답이 모두 돌아오고 stream_id가 1, 3, 5...로 늘어난다 (규격 4.2)")
    void sequentialRequestsKeepStreamNumbersAndResponses() throws Exception {
        try (TestServer server = TestServer.startResponding(type -> STATUS_OK);
                TestClient client = TestClient.connect(server.port())) {
            Set<Integer> resultStreams = new HashSet<>();
            int sent = 0;
            int received = 0;
            while (received < TOTAL_REQUESTS) {
                while (sent < TOTAL_REQUESTS && sent - received < MAX_OUTSTANDING) {
                    client.request(APPLICATION_TYPE, Fixtures.payload("request-" + sent));
                    sent++;
                }
                Event result = client.awaitEvent(Event.Kind.RESULT);
                assertThat(result.status()).as("%d번째 결과의 status", received).isEqualTo(STATUS_OK);
                resultStreams.add(result.streamId());
                received++;
            }

            Set<Integer> expected = new HashSet<>();
            for (int i = 0; i < TOTAL_REQUESTS; i++) {
                expected.add(2 * i + 1);
            }
            assertThat(resultStreams).isEqualTo(expected);
        }
    }
}
