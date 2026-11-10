package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.nio.internal.IncomingRequest;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
     * 요청을 이어 보내는 동안 스트림 번호가 어긋나거나 응답이 다른 요청에 매핑되거나 유실되면 결과가 모자라거나 요청별 {@code status}가 달라진다. 클라이언트
     * 번호 공간은 1, 3, 5...로 늘어나므로(규격 4.2절) {@code i}번째 요청은 {@code stream_id} {@code 2i + 1}을 받는다. 요청 {@value #TOTAL_REQUESTS}개
     * 모두에 대해 그 요청의 결과 {@code status}가 {@code i % 8}이어야 하고, 서버가 받은 그 요청 본문의 스트림 번호가 {@code 2i + 1}이어야 한다.
     */
    @Test
    @DisplayName("요청 1,000개를 이어 보내도 각 stream_id의 응답이 자기 요청에 매핑된다 (규격 4.2)")
    void sequentialRequestsKeepStreamNumbersAndResponses() throws Exception {
        try (TcpServer server = TcpServer.start(type -> (type - APPLICATION_TYPE));
                Peer client = Peer.connect(server.port(), null)) {
            Peer serverSide = server.awaitPeer();
            List<CompletableFuture<RequestResult>> futures = new ArrayList<>();
            int received = 0;
            while (received < TOTAL_REQUESTS) {
                while (futures.size() < TOTAL_REQUESTS && futures.size() - received < MAX_OUTSTANDING) {
                    int index = futures.size();
                    futures.add(client.request(
                                    APPLICATION_TYPE + index % STATUS_CYCLE, Fixtures.payload("request-" + index))
                            .result());
                }
                RequestResult result = futures.get(received).get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
                assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
                assertThat(result.status()).as("%d번째 요청의 status", received).isEqualTo(received % STATUS_CYCLE);
                received++;
            }

            Map<String, Long> streamByBody = new HashMap<>();
            for (IncomingRequest request : serverSide.drainRequests()) {
                streamByBody.put(new String(request.body(), StandardCharsets.UTF_8), request.streamId());
            }
            assertThat(streamByBody).hasSize(TOTAL_REQUESTS);
            for (int i = 0; i < TOTAL_REQUESTS; i++) {
                assertThat(streamByBody).containsEntry("request-" + i, 2L * i + 1);
            }
        }
    }
}
