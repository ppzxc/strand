package io.github.ppzxc.strand.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Event;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 규격 3.1~3.3절 연결 수립을 실제 TCP 위에서 확인한다. */
class HandshakeIntegrationTest {

    private static final int APPLICATION_TYPE = 256;

    /**
     * 클라이언트는 서버 HELLO를 ACK하기 전의 요청을 미뤘다가 수립 뒤에 보낸다. 서문, HELLO 교환, ACK 중 하나라도 실제 연결에서 어긋나면 요청이 서버에
     * 도달하지 않아 이 테스트가 시간 초과로 실패한다.
     */
    @Test
    @DisplayName("TCP 연결을 열고 바로 낸 요청이 핸드셰이크 뒤 서버에 도달한다 (규격 3.1~3.3)")
    void requestSentBeforeHandshakeReachesServer() throws Exception {
        try (TestServer server = TestServer.start();
                TestClient client = TestClient.connect(server.port())) {
            client.request(APPLICATION_TYPE, "hello".getBytes(StandardCharsets.UTF_8));

            Event received = server.awaitConnection().awaitEvent(Event.Kind.REQUEST_RECEIVED);

            assertThat(received.type()).isEqualTo(APPLICATION_TYPE);
        }
    }
}
