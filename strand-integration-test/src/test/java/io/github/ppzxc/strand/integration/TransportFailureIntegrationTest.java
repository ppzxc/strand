package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Event;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 전송 계층이 CLOSE 없이 끊기거나 바이트가 쪼개져 오는 경우를 실제 TCP 위에서 확인한다. */
class TransportFailureIntegrationTest {

    /** 규격 4.2절: 클라이언트가 처음 여는 스트림은 1번이다. */
    private static final int FIRST_CLIENT_STREAM_ID = 1;

    /**
     * 응답을 기다리는 중에 상대가 CLOSE 없이 TCP를 끊으면, 대기 중이던 요청은 로컬 {@code UNAVAILABLE}로 정확히 한 번 끝나고 그 뒤에 {@code
     * closed}가 한 번 나야 한다(규격 3.5절). 이 처리가 빠지면 애플리케이션이 응답을 영영 기다리고, 중복되면 같은 요청이 두 번 실패로 처리된다.
     */
    @Test
    @DisplayName("CLOSE 없이 TCP가 끊기면 대기 요청이 UNAVAILABLE로 한 번 끝나고 closed가 한 번 난다 (규격 3.5)")
    void abruptDisconnectFailsPendingRequestOnce() throws Exception {
        try (TestServer server = TestServer.start();
                TestClient client = TestClient.connect(server.port())) {
            Connection serverConnection = server.awaitConnection();
            client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
            serverConnection.awaitEvent(Event.Kind.REQUEST_RECEIVED);

            serverConnection.close();
            client.awaitFinished();

            List<Event> events = client.events();
            assertThat(events).extracting(Event::kind).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
            assertThat(events.get(0).streamId()).isEqualTo(FIRST_CLIENT_STREAM_ID);
            assertThat(events.get(0).status()).isEqualTo(STATUS_UNAVAILABLE);
        }
    }

    /**
     * TCP는 바이트 스트림이라 프레임이 여러 번의 읽기에 나뉘어 올 수 있다. 읽기를 1바이트로 제한한 양쪽 연결에서도 서문, HELLO, 요청, 응답이 하나씩
     * 조립되어야 한다(규격 2.1절 "프레임 하나가 곧 메시지 하나"). 프레임 경계를 읽기 한 번에 맞춘다고 가정한 구현은 요청이 도달하지 않아 시간 초과로
     * 실패한다.
     */
    @Test
    @DisplayName("1바이트씩 읽어도 프레임이 조립되어 요청과 응답이 오간다 (규격 2.1)")
    void framesAreReassembledFromOneByteReads() throws Exception {
        int oneByte = 1;
        try (TestServer server = TestServer.startResponding(type -> STATUS_OK, oneByte);
                TestClient client = TestClient.connect(server.port(), oneByte)) {
            Connection serverConnection = server.awaitConnection();
            client.request(APPLICATION_TYPE, Fixtures.payload("hello"));

            Event received = serverConnection.awaitEvent(Event.Kind.REQUEST_RECEIVED);
            Event result = client.awaitEvent(Event.Kind.RESULT);

            assertThat(received.type()).isEqualTo(APPLICATION_TYPE);
            assertThat(result.streamId()).isEqualTo(received.streamId());
            assertThat(result.status()).isEqualTo(STATUS_OK);
        }
    }
}
