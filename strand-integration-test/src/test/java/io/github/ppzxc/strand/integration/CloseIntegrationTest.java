package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.NO_ERROR;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Event;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 규격 3.5절 연결 종료를 실제 TCP 위에서 확인한다. */
class CloseIntegrationTest {

    /**
     * 대기 중인 스트림이 없을 때 {@code NO_ERROR} CLOSE를 보내면 양쪽이 {@code closed}를 정확히 한 번씩 낸다. {@code closed}가 빠지면
     * 애플리케이션이 종료를 알지 못한다. 하니스는 {@code closed} 뒤에도 상대가 끊을 때까지 읽으므로, 닫힌 뒤에 감지한 TCP 끊김이 {@code closed}를
     * 한 번 더 내면 개수가 2가 되어 실패한다(규격 3.5절 "연결당 정확히 한 번").
     */
    @Test
    @DisplayName("NO_ERROR CLOSE로 끝내면 양쪽이 closed를 정확히 한 번씩 낸다 (규격 3.5)")
    void normalCloseEmitsClosedOncePerSide() throws Exception {
        try (TestServer server = TestServer.startResponding(type -> STATUS_OK);
                TestClient client = TestClient.connect(server.port())) {
            Connection serverConnection = server.awaitConnection();
            client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
            serverConnection.awaitEvent(Event.Kind.REQUEST_RECEIVED);
            client.awaitEvent(Event.Kind.RESULT);

            client.closeConnection(NO_ERROR);
            serverConnection.awaitFinished();
            client.awaitFinished();

            assertThat(closedCount(serverConnection.events())).isEqualTo(1);
            assertThat(closedCount(client.events())).isEqualTo(1);
        }
    }

    private static long closedCount(List<Event> events) {
        return events.stream().filter(e -> e.kind() == Event.Kind.CLOSED).count();
    }
}
