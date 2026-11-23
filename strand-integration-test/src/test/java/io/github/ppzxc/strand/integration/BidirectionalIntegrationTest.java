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

/** 한 연결에서 양쪽이 서로 요청을 내는 경우를 실제 TCP 위에서 확인한다. */
@ExtendWith(PassageObservation.class)
class BidirectionalIntegrationTest {

    private static final int REQUESTS_PER_SIDE = 3;
    /**
     * 요청 {@code type}은 {@code APPLICATION_TYPE + i}(i는 0부터)다. 서버는 {@code i}를 그대로 {@code status}로, 클라이언트는 {@code i +
     * 3}을 {@code status}로 응답한다. 방향마다 {@code status} 범위가 달라서 결과가 어느 방향의 응답인지, 몇 번째 요청의 응답인지 가를 수 있다.
     */
    private static final int CLIENT_STATUS_OFFSET = REQUESTS_PER_SIDE;

    private static RequestResult await(CompletableFuture<RequestResult> future) throws Exception {
        return future.get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * 요청과 응답은 번호 공간으로만 구분하므로(규격 4.2절), 양쪽이 번갈아 요청을 내도 한 연결 안에서 홀수 번호(클라이언트 개시: 1, 3, 5)와 짝수 번호(서버 개시:
     * 2, 4, 6)가 섞이지 않아야 한다. 번호 공간을 혼동하면 도착한 요청의 {@code stream_id}나 결과의 {@code status}가 어긋난다. 요청을 세 건씩 번갈아 내서 같은
     * 방향의 요청이 여러 건일 때도 번호가 2씩 늘어나는지, 각 결과가 자기 요청의 응답인지 함께 본다.
     */
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
            // 스트림 번호는 통로를 관찰할 수 있는 전송(nio)에서만 본다. netty에서는 위의 방향별 응답 매칭까지만 본다.
            if (!transport.observesPassage()) {
                return;
            }
            // 요청을 받은 쪽 통로가 읽은 프레임의 번호로 본다. 제품 API는 받은 요청의 스트림 번호를 드러내지 않는다.
            for (int i = 0; i < REQUESTS_PER_SIDE; i++) {
                serverSide.awaitRequest();
                client.awaitRequest();
            }
            Map<Long, Integer> typeByStreamAtServer =
                    Fixtures.requestTypeByStream(serverSide.passage().readFrames(), true);
            Map<Long, Integer> typeByStreamAtClient =
                    Fixtures.requestTypeByStream(client.passage().readFrames(), false);

            // 클라이언트 개시: 번호 1, 3, 5. 서버 개시: 번호 2, 4, 6. 번호 i번째 요청의 type이 APPLICATION_TYPE + i다.
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
