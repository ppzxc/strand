package io.github.ppzxc.strand.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Event;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 규격 3.5절 연결 종료를 실제 TCP 위에서 확인한다. */
class CloseIntegrationTest {

    private static final int APPLICATION_TYPE = 256;
    private static final int STATUS_OK = 0;
    private static final int NO_ERROR = 0;

    /**
     * 대기 중인 스트림이 없을 때 {@code NO_ERROR} CLOSE를 보내면 양쪽이 연결을 닫고 {@code closed}를 정확히 한 번씩 낸다. {@code closed}가
     * 빠지면 애플리케이션이 종료를 알지 못하고, 두 번 나면 종료 처리가 중복된다. 연결이 끝난 뒤에도 소켓이 열려 있으면 자원이 샌다.
     */
    @Test
    @DisplayName("NO_ERROR CLOSE로 끝내면 양쪽이 closed를 한 번씩 내고 소켓이 닫힌다 (규격 3.5)")
    void normalCloseEndsBothSidesOnce() throws Exception {
        try (TestServer server = TestServer.startResponding(STATUS_OK);
                TestClient client = TestClient.connect(server.port())) {
            Peer serverPeer = server.awaitConnection();
            client.request(APPLICATION_TYPE, "hello".getBytes(StandardCharsets.UTF_8));
            serverPeer.awaitEvent(Event.Kind.REQUEST_RECEIVED);
            client.awaitEvent(Event.Kind.RESULT);

            client.closeConnection(NO_ERROR);
            serverPeer.awaitFinished();
            client.peer().awaitFinished();

            assertThat(closedCount(serverPeer.events())).isEqualTo(1);
            assertThat(closedCount(client.peer().events())).isEqualTo(1);
            assertThat(serverPeer.isSocketClosed()).isTrue();
            assertThat(client.peer().isSocketClosed()).isTrue();
        }
    }

    private static long closedCount(List<Event> events) {
        return events.stream().filter(e -> e.kind() == Event.Kind.CLOSED).count();
    }
}
