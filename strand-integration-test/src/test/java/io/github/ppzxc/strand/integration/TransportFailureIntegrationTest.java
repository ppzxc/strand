package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.Event;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 전송 계층이 CLOSE 없이 끊기거나 바이트가 쪼개져 오는 경우를 실제 TCP 위에서 확인한다. */
class TransportFailureIntegrationTest {

    /** 규격 4.2절: 클라이언트가 처음 여는 스트림은 1번이다. */
    private static final long FIRST_CLIENT_STREAM_ID = 1L;

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
            assertThat(events.get(0).certainty()).contains(Certainty.UNKNOWN);
        }
    }

    /**
     * 요청 프레임을 전송 계층에 넘기지 못한 채 연결이 끝나면 상대가 요청을 보지 못했음이 확실하므로 처리 안 됨이다(규격 3.5절). 서버가 HELLO에
     * 답하지 않아 AUTH 관문이 열리지 않은 연결에서 요청을 내면 프레임은 쓰이지 않고, 서버가 TCP를 끊으면 {@code stream_id} 없는 로컬 {@code
     * UNAVAILABLE}과 처리 안 됨이 한 번 나온다.
     */
    @Test
    @DisplayName("프레임을 보내지 못한 요청은 연결이 끝날 때 처리 안 됨으로 끝난다 (규격 3.5)")
    void requestNeverWrittenEndsAsNotProcessed() throws Exception {
        try (ServerSocket silentServer = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                TestClient client = TestClient.connect(silentServer.getLocalPort())) {
            Socket accepted = silentServer.accept();
            client.request(APPLICATION_TYPE, Fixtures.payload("hello"));

            accepted.close();
            client.awaitFinished();

            List<Event> events = client.events();
            assertThat(events).extracting(Event::kind).containsExactly(Event.Kind.RESULT, Event.Kind.CLOSED);
            assertThat(events.get(0).streamId()).isZero();
            assertThat(events.get(0).status()).isEqualTo(STATUS_UNAVAILABLE);
            assertThat(events.get(0).certainty()).contains(Certainty.NOT_PROCESSED);
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
