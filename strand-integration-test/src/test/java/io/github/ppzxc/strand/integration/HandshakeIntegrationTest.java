package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.nio.internal.IncomingRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 규격 3.1~3.3절 연결 수립을 실제 TCP 위에서 확인한다. 서문과 HELLO, ACK의 바이트 형식은 컨포먼스 벡터가 맡고, 여기서는 수립이 끝나야 비로소 풀리는
 * 동작이 실제 연결에서 일어나는지만 본다.
 */
class HandshakeIntegrationTest {

    /**
     * 클라이언트는 서버 HELLO를 ACK하기 전의 요청을 미뤘다가 수립 뒤에 보낸다. 서문, HELLO 교환, ACK 중 하나라도 실제 연결에서 어긋나면 요청이 서버에
     * 도달하지 않아 시간 초과로 실패한다.
     */
    @Test
    @DisplayName("TCP 연결을 열고 바로 낸 요청이 핸드셰이크 뒤 서버에 도달한다 (규격 3.1~3.3)")
    void requestSentBeforeHandshakeReachesServer() throws Exception {
        try (TcpServer server = TcpServer.start();
                Peer client = Peer.connect(server.port(), null)) {
            Peer serverSide = server.awaitPeer();
            client.request(APPLICATION_TYPE, Fixtures.payload("hello"));

            IncomingRequest received = serverSide.awaitRequest();

            assertThat(received.type()).isEqualTo(APPLICATION_TYPE);
            assertThat(received.body()).isEqualTo(Fixtures.payload("hello"));
        }
    }
}
