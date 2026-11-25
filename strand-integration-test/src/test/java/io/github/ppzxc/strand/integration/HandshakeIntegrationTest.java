package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;

class HandshakeIntegrationTest {

    @TransportTest
    @DisplayName("TCP 연결을 열고 바로 낸 요청이 핸드셰이크 뒤 서버에 도달한다 (규격 3.1~3.3)")
    void requestSentBeforeHandshakeReachesServer(Transport transport) throws Exception {
        try (TcpServer server = TcpServer.start(transport);
                Peer client = Peer.connect(transport, server.port(), null)) {
            Peer serverSide = server.awaitPeer();
            client.request(APPLICATION_TYPE, Fixtures.payload("hello"));

            Wire.Incoming received = serverSide.awaitRequest();

            assertThat(received.type()).isEqualTo(APPLICATION_TYPE);
            assertThat(received.payload()).isEqualTo(Fixtures.payload("hello"));
        }
    }
}
