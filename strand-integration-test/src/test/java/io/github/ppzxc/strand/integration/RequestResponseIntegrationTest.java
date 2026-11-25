package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.RequestResult;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(PassageObservation.class)
class RequestResponseIntegrationTest {

    private static final int FIRST_TYPE = APPLICATION_TYPE;
    private static final int SECOND_TYPE = APPLICATION_TYPE + 1;

    private static final int STATUS_FOR_FIRST = 0;

    private static final int STATUS_FOR_SECOND = 1;

    @TransportTest
    @DisplayName("겹쳐 낸 요청마다 응답이 자기 stream_id로 돌아온다 (규격 4.2)")
    void eachOverlappingRequestGetsItsOwnResponse(Transport transport) throws Exception {
        try (TcpServer server =
                        TcpServer.start(transport, type -> type == FIRST_TYPE ? STATUS_FOR_FIRST : STATUS_FOR_SECOND);
                Peer client = Peer.connect(transport, server.port(), null)) {
            Peer serverSide = server.awaitPeer();
            CompletableFuture<RequestResult> first =
                    client.request(FIRST_TYPE, Fixtures.payload("first")).result();
            CompletableFuture<RequestResult> second =
                    client.request(SECOND_TYPE, Fixtures.payload("second")).result();

            RequestResult firstResult = first.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
            RequestResult secondResult = second.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);

            assertThat(firstResult.certainty()).isEqualTo(Certainty.RESPONSE);
            assertThat(firstResult.status()).isEqualTo(STATUS_FOR_FIRST);
            assertThat(secondResult.certainty()).isEqualTo(Certainty.RESPONSE);
            assertThat(secondResult.status()).isEqualTo(STATUS_FOR_SECOND);

            if (!transport.observesPassage()) {
                return;
            }

            Map<Integer, Long> streamByType = new java.util.HashMap<>();
            Fixtures.requestTypeByStream(serverSide.passage().readFrames(), true)
                    .forEach((streamId, type) -> streamByType.put(type, streamId));
            assertThat(streamByType).containsOnly(entry(FIRST_TYPE, 1L), entry(SECOND_TYPE, 3L));
        }
    }

    @TransportTest
    @DisplayName("응답을 받은 요청의 결과 확정성은 응답 받음이다 (규격 1.3)")
    void answeredRequestHasResponseCertainty(Transport transport) throws Exception {
        try (TcpServer server = TcpServer.start(transport, type -> STATUS_FOR_FIRST);
                Peer client = Peer.connect(transport, server.port(), null)) {
            RequestResult result = client.request(FIRST_TYPE, Fixtures.payload("first"))
                    .result()
                    .get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);

            assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
            assertThat(result.payload()).isEqualTo(Fixtures.payload("ok"));
        }
    }
}
