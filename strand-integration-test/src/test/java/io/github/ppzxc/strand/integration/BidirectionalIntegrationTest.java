package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.RequestResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(PassageObservation.class)
class BidirectionalIntegrationTest {

    private static final int REQUESTS_PER_SIDE = 3;

    private static final int CLIENT_STATUS_OFFSET = REQUESTS_PER_SIDE;

    private static RequestResult await(CompletableFuture<RequestResult> future) throws Exception {
        return future.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @TransportTest
    @DisplayName("서버와 클라이언트가 번갈아 요청을 내도 홀수·짝수 stream_id가 섞이지 않는다 (규격 4.2)")
    void bothSidesRequestWithoutMixingStreamNumbers(Transport transport) throws Exception {
        try (TcpServer server = TcpServer.start(transport, type -> type - APPLICATION_TYPE);
                Peer client = Peer.connect(
                        transport, server.port(), type -> type - APPLICATION_TYPE + CLIENT_STATUS_OFFSET)) {
            Peer serverSide = server.awaitPeer();
            List<CompletableFuture<RequestResult>> fromClient = new ArrayList<>();
            List<CompletableFuture<RequestResult>> fromServer = new ArrayList<>();
            for (int i = 0; i < REQUESTS_PER_SIDE; i++) {
                fromClient.add(client.request(APPLICATION_TYPE + i, Fixtures.payload("from-client-" + i))
                        .result());
                fromServer.add(serverSide
                        .request(APPLICATION_TYPE + i, Fixtures.payload("from-server-" + i))
                        .result());
            }

            for (int i = 0; i < REQUESTS_PER_SIDE; i++) {
                RequestResult clientResult = await(fromClient.get(i));
                RequestResult serverResult = await(fromServer.get(i));
                assertThat(clientResult.certainty()).isEqualTo(Certainty.RESPONSE);
                assertThat(serverResult.certainty()).isEqualTo(Certainty.RESPONSE);
                assertThat(clientResult.status()).isEqualTo(i);
                assertThat(serverResult.status()).isEqualTo(i + CLIENT_STATUS_OFFSET);
            }

            if (!transport.observesPassage()) {
                return;
            }

            for (int i = 0; i < REQUESTS_PER_SIDE; i++) {
                serverSide.awaitRequest();
                client.awaitRequest();
            }
            Map<Long, Integer> typeByStreamAtServer =
                    Fixtures.requestTypeByStream(serverSide.passage().readFrames(), true);
            Map<Long, Integer> typeByStreamAtClient =
                    Fixtures.requestTypeByStream(client.passage().readFrames(), false);

            assertThat(typeByStreamAtServer)
                    .containsOnly(
                            entry(1L, APPLICATION_TYPE),
                            entry(3L, APPLICATION_TYPE + 1),
                            entry(5L, APPLICATION_TYPE + 2));
            assertThat(typeByStreamAtClient)
                    .containsOnly(
                            entry(2L, APPLICATION_TYPE),
                            entry(4L, APPLICATION_TYPE + 1),
                            entry(6L, APPLICATION_TYPE + 2));
        }
    }
}
