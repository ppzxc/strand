package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.nio.internal.ConnectionClosed;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 규격 3.5절 연결 종료를 실제 TCP 위에서 확인한다. */
class CloseIntegrationTest {

    /**
     * 대기 중인 스트림이 없을 때 {@code NO_ERROR} CLOSE를 보내면 양쪽이 종료 알림을 정확히 한 번씩 받는다. 알림이 빠지면 애플리케이션이 종료를 알지
     * 못한다. 전송 모듈은 종료 뒤에도 상대가 끊을 때까지 읽으므로, 닫힌 뒤에 감지한 TCP 끊김이 알림을 한 번 더 내면 연결을 모두 닫은 뒤의 알림 수가 2가 되어
     * 실패한다(규격 3.5절 "연결당 정확히 한 번"). 종료 원인은 보낸 쪽이 LOCAL, 받은 쪽이 PEER인 CLOSE다.
     */
    @Test
    @DisplayName("NO_ERROR CLOSE로 끝내면 양쪽이 종료 알림을 정확히 한 번씩 받는다 (규격 3.5)")
    void normalCloseNotifiesOncePerSide() throws Exception {
        try (TcpServer server = TcpServer.start(type -> STATUS_OK);
                Peer client = Peer.connect(server.port(), null)) {
            Peer serverSide = server.awaitPeer();
            client.request(APPLICATION_TYPE, Fixtures.payload("hello"))
                    .result()
                    .get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);

            client.driver().closeConnection();
            ConnectionClosed atClient = client.awaitClosed();
            ConnectionClosed atServer = serverSide.awaitClosed();
            client.stop();
            serverSide.stop();

            assertThat(client.closedNotices()).hasSize(1);
            assertThat(serverSide.closedNotices()).hasSize(1);
            assertThat(atClient.reason().kind()).isEqualTo(CloseReason.Kind.CLOSE);
            assertThat(atClient.reason().sender()).contains(CloseReason.Sender.LOCAL);
            assertThat(atServer.reason().kind()).isEqualTo(CloseReason.Kind.CLOSE);
            assertThat(atServer.reason().sender()).contains(CloseReason.Sender.PEER);
        }
    }
}
