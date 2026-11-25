package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.CloseNotice;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;

class CloseIntegrationTest {

    @TransportTest
    @DisplayName("NO_ERROR CLOSE로 끝내면 양쪽이 종료 알림을 정확히 한 번씩 받는다 (규격 3.5)")
    void normalCloseNotifiesOncePerSide(Transport transport) throws Exception {
        try (TcpServer server = TcpServer.start(transport, type -> STATUS_OK);
                Peer client = Peer.connect(transport, server.port(), null)) {
            Peer serverSide = server.awaitPeer();
            client.request(APPLICATION_TYPE, Fixtures.payload("hello"))
                    .result()
                    .get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);

            client.closeConnection();
            CloseNotice atClient = client.awaitClosed();
            CloseNotice atServer = serverSide.awaitClosed();
            client.stop();
            serverSide.stop();

            assertThat(client.closedNotices()).hasSize(1);
            assertThat(serverSide.closedNotices()).hasSize(1);
            assertThat(atClient.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
            assertThat(atClient.sender()).contains(CloseNotice.Sender.LOCAL);
            assertThat(atServer.kind()).isEqualTo(CloseNotice.Kind.CLOSE);
            assertThat(atServer.sender()).contains(CloseNotice.Sender.PEER);
        }
    }
}
