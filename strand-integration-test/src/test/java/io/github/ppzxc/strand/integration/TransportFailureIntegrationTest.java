package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.CloseReason;
import io.github.ppzxc.strand.nio.internal.ConnectionClosed;
import io.github.ppzxc.strand.nio.internal.IncomingRequest;
import io.github.ppzxc.strand.nio.internal.RequestHandle;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 전송 계층이 CLOSE 없이 끊기거나 바이트가 쪼개져 오는 경우를 실제 TCP 위에서 확인한다. */
class TransportFailureIntegrationTest {

    private static RequestResult await(RequestHandle handle) throws Exception {
        return handle.result().get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * 응답을 기다리는 중에 상대가 CLOSE 없이 TCP를 끊으면, 대기 중이던 요청은 로컬 {@code UNAVAILABLE}과 결과 모름으로 끝나고 그 뒤에 종료 알림이 CLOSE
     * 없이 끊김으로 한 번 와야 한다(규격 3.5절). 이 처리가 빠지면 애플리케이션이 응답을 영영 기다리고, 알림이 중복되면 같은 연결의 종료를 두 번 처리한다.
     */
    @Test
    @DisplayName("CLOSE 없이 TCP가 끊기면 대기 요청이 UNAVAILABLE로 끝나고 종료 알림이 한 번 온다 (규격 3.5)")
    void abruptDisconnectFailsPendingRequestOnce() throws Exception {
        try (TcpServer server = TcpServer.start();
                Peer client = Peer.connect(server.port(), null)) {
            Peer serverSide = server.awaitPeer();
            RequestHandle handle = client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
            client.logResult(handle.result());
            serverSide.awaitRequest();

            serverSide.stop();
            RequestResult result = await(handle);
            ConnectionClosed closed = client.awaitClosed();
            client.awaitResultsLogged();
            client.stop();

            assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
            assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
            assertThat(closed.reason().kind()).isEqualTo(CloseReason.Kind.DISCONNECTED);
            assertThat(closed.ioException()).isPresent();
            assertThat(client.closedNotices()).hasSize(1);
            // 결과가 종료 알림보다 먼저다. 요청 하나가 결과를 정확히 한 번 받고 알림이 그 뒤에 한 번 온다.
            assertThat(client.log()).containsExactly(Peer.RESULT, Peer.CLOSED);
        }
    }

    /**
     * 요청 프레임을 전송 계층에 넘기지 못한 채 연결이 끝나면 상대가 요청을 보지 못했음이 확실하므로 처리 안 됨이다(규격 3.5절). 서버가 HELLO에
     * 답하지 않아 AUTH 관문이 열리지 않은 연결에서 요청을 내면 프레임은 쓰이지 않고, 서버가 TCP를 끊으면 로컬 {@code UNAVAILABLE}과 처리 안 됨으로 끝난다.
     */
    @Test
    @DisplayName("프레임을 보내지 못한 요청은 연결이 끝날 때 처리 안 됨으로 끝난다 (규격 3.5)")
    void requestNeverWrittenEndsAsNotProcessed() throws Exception {
        try (ServerSocket silentServer = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                Peer client = Peer.connect(silentServer.getLocalPort(), null)) {
            Socket accepted = silentServer.accept();
            RequestHandle handle = client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
            client.logResult(handle.result());

            accepted.close();
            RequestResult result = await(handle);
            client.awaitClosed();
            // 결과를 적는 동작은 결과 Future를 끝낸 스레드에서, 종료 알림은 핸들러 실행기에서 돈다. 둘의 순서는 정해져 있지 않으므로
            // 둘 다 적힌 뒤에 순서와 무관하게 본다.
            client.awaitResultsLogged();
            client.stop();

            assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
            assertThat(result.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
            assertThat(client.closedNotices()).hasSize(1);
            assertThat(client.log()).containsExactlyInAnyOrder(Peer.RESULT, Peer.CLOSED);
            // 결과 Future는 스트림 번호를 드러내지 않아 "스트림 번호 미배정"은 볼 수 없다. 대신 요청 프레임이 통로에 쓰이지 않았음을 보낸 프레임 수로 본다.
            // 말 없는 서버에 클라이언트가 쓴 것은 HELLO 하나뿐이다.
            assertThat(client.driver().writtenFrames()).isEqualTo(1);
        }
    }

    /**
     * TCP는 바이트 스트림이라 프레임이 여러 번의 읽기에 나뉘어 올 수 있다. 읽기를 1바이트로 제한한 양쪽 연결에서도 서문, HELLO, 요청, 응답이 하나씩
     * 조립되어야 한다(규격 2.1절 "프레임 하나가 곧 메시지 하나"). 프레임 경계를 읽기 한 번에 맞춘다고 가정한 구현은 요청이 도달하지 않아 시간 초과로
     * 실패한다. 읽은 바이트 수도 관찰 지점으로 확인해, 1바이트씩 읽었어도 보낸 만큼 모두 처리됐는지 본다.
     */
    @Test
    @DisplayName("1바이트씩 읽어도 프레임이 조립되어 요청과 응답이 오간다 (규격 2.1)")
    void framesAreReassembledFromOneByteReads() throws Exception {
        int oneByte = 1;
        try (TcpServer server = TcpServer.start(type -> STATUS_OK, channel -> Peer.tcp(channel, oneByte));
                Peer client = Peer.connect(server.port(), null, oneByte)) {
            Peer serverSide = server.awaitPeer();

            RequestResult result = await(client.request(APPLICATION_TYPE, Fixtures.payload("hello")));
            IncomingRequest received = serverSide.awaitRequest();

            assertThat(received.type()).isEqualTo(APPLICATION_TYPE);
            assertThat(received.body()).isEqualTo(Fixtures.payload("hello"));
            assertThat(result.status()).isEqualTo(STATUS_OK);
            assertThat(result.body()).isEqualTo(Fixtures.payload("ok"));
            serverSide.awaitReceivedBytes(client.driver().writtenBytes());
            client.awaitReceivedBytes(serverSide.driver().writtenBytes());
        }
    }
}
