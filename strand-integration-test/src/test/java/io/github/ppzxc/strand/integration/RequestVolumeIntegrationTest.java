package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(PassageObservation.class)
class RequestVolumeIntegrationTest {

    private static final int TOTAL_REQUESTS = 1000;

    private static final int MAX_OUTSTANDING = 50;

    private static final int STATUS_CYCLE = 8;

    @TransportTest
    @DisplayName("요청 1,000개를 이어 보내도 각 stream_id의 응답이 자기 요청에 매핑된다 (규격 4.2)")
    void sequentialRequestsKeepStreamNumbersAndResponses(Transport transport) throws Exception {
        try (TcpServer server = TcpServer.start(transport, type -> (type - APPLICATION_TYPE));
                Peer client = Peer.connect(transport, server.port(), null)) {
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

            if (!transport.observesPassage()) {
                return;
            }
            Map<String, Long> streamByBody = new HashMap<>();

            for (ControlledPassage.Frame frame : serverSide.passage().readFrames()) {
                if (frame.type() >= APPLICATION_TYPE) {
                    streamByBody.put(new String(frame.payload(), StandardCharsets.UTF_8), frame.streamId());
                }
            }
            assertThat(streamByBody).hasSize(TOTAL_REQUESTS);
            for (int i = 0; i < TOTAL_REQUESTS; i++) {
                assertThat(streamByBody).containsEntry("request-" + i, 2L * i + 1);
            }
        }
    }
}
