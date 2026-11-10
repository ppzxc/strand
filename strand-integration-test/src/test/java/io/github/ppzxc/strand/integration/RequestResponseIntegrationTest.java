package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.nio.internal.IncomingRequest;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 규격 4.2절 스트림 식별을 실제 TCP 위에서 확인한다. */
class RequestResponseIntegrationTest {

    private static final int FIRST_TYPE = APPLICATION_TYPE;
    private static final int SECOND_TYPE = APPLICATION_TYPE + 1;
    /** 서버는 {@link #FIRST_TYPE} 요청에 이 status로, {@link #SECOND_TYPE} 요청에 다른 status로 응답한다. */
    private static final int STATUS_FOR_FIRST = 0;

    private static final int STATUS_FOR_SECOND = 1;

    /**
     * 응답이 요청을 낸 스트림에 매칭되지 않으면 클라이언트는 결과를 받지 못하거나 다른 요청의 결과로 처리한다. 요청 하나로는 이 오매칭을 구분할 수
     * 없어서, 응답을 기다리지 않고 요청 두 개를 겹쳐 낸다. 서버는 요청 {@code type}에 따라 서로 다른 {@code status}로 응답하므로, 각 요청의 Future가
     * 자기 {@code status}로 끝났는지 알 수 있다. 서버가 받은 요청의 스트림 번호는 클라이언트 번호 공간 1, 3, 5...여야 한다(규격 4.2절).
     */
    @Test
    @DisplayName("겹쳐 낸 요청마다 응답이 자기 stream_id로 돌아온다 (규격 4.2)")
    void eachOverlappingRequestGetsItsOwnResponse() throws Exception {
        try (TcpServer server = TcpServer.start(type -> type == FIRST_TYPE ? STATUS_FOR_FIRST : STATUS_FOR_SECOND);
                Peer client = Peer.connect(server.port(), null)) {
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
            Map<Integer, Long> streamByType = new HashMap<>();
            for (IncomingRequest request : serverSide.drainRequests()) {
                streamByType.put(request.type(), request.streamId());
            }
            assertThat(streamByType).containsOnly(entry(FIRST_TYPE, 1L), entry(SECOND_TYPE, 3L));
        }
    }

    /** 상대의 응답을 받아 끝난 요청의 결과 확정성은 응답 받음이고 응답 본문이 함께 온다(규격 1.3절, 4.1절). */
    @Test
    @DisplayName("응답을 받은 요청의 결과 확정성은 응답 받음이다 (규격 1.3)")
    void answeredRequestHasResponseCertainty() throws Exception {
        try (TcpServer server = TcpServer.start(type -> STATUS_FOR_FIRST);
                Peer client = Peer.connect(server.port(), null)) {
            RequestResult result = client.request(FIRST_TYPE, Fixtures.payload("first"))
                    .result()
                    .get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);

            assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
            assertThat(result.body()).isEqualTo(Fixtures.payload("ok"));
        }
    }
}
