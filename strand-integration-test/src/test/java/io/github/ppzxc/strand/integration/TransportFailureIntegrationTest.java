package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_OK;
import static io.github.ppzxc.strand.integration.Fixtures.STATUS_UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.api.Certainty;
import io.github.ppzxc.strand.api.CloseNotice;
import io.github.ppzxc.strand.api.RequestResult;
import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(PassageObservation.class)
class TransportFailureIntegrationTest {

    private static RequestResult await(Peer.Sent sent) throws Exception {
        return sent.result().get(Fixtures.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @TransportTest
    @DisplayName("CLOSE 없이 TCP가 끊기면 대기 요청이 UNAVAILABLE로 끝나고 종료 알림이 한 번 온다 (규격 3.5)")
    void abruptDisconnectFailsPendingRequestOnce(Transport transport) throws Exception {
        try (TcpServer server = TcpServer.start(transport);
                Peer client = Peer.connect(transport, server.port(), null)) {
            Peer serverSide = server.awaitPeer();
            Peer.Sent handle = client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
            client.logResult(handle);
            serverSide.awaitRequest();

            serverSide.stop();
            RequestResult result = await(handle);
            CloseNotice closed = client.awaitClosed();
            client.awaitResultsLogged();
            client.stop();

            assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
            assertThat(result.certainty()).isEqualTo(Certainty.UNKNOWN);
            assertThat(closed.kind()).isEqualTo(CloseNotice.Kind.DISCONNECTED);
            assertThat(closed.cause()).isPresent();
            assertThat(client.closedNotices()).hasSize(1);

            assertThat(client.log()).containsExactly(Peer.RESULT, Peer.CLOSED);
        }
    }

    @Test
    @DisplayName("[nio 전용] 프레임을 보내지 못한 요청은 연결이 끝날 때 처리 안 됨으로 끝난다 (규격 3.5)")
    void requestNeverWrittenEndsAsNotProcessed() throws Exception {
        Transport transport = Transport.NIO;
        try (TcpServer server = TcpServer.start(transport);
                Peer client = Peer.connect(transport, server.port(), null)) {
            Peer serverSide = server.awaitPeer();
            ControlledPassage passage = client.passage();

            passage.writeCapacity(0);
            int framesBefore = passage.writtenFrames().size();
            Peer.Sent handle = client.request(APPLICATION_TYPE, Fixtures.payload("hello"));
            client.logResult(handle);

            serverSide.stop();
            RequestResult result = await(handle);
            client.awaitClosed();

            client.awaitResultsLogged();
            client.stop();

            assertThat(result.status()).isEqualTo(STATUS_UNAVAILABLE);
            assertThat(result.certainty()).isEqualTo(Certainty.NOT_PROCESSED);
            assertThat(client.closedNotices()).hasSize(1);
            assertThat(client.log()).containsExactlyInAnyOrder(Peer.RESULT, Peer.CLOSED);

            assertThat(passage.writtenFrames()).hasSize(framesBefore);
        }
    }

    @Test
    @DisplayName("[nio 전용] 1바이트씩 읽어도 프레임이 조립되어 요청과 응답이 오간다 (규격 2.1)")
    void framesAreReassembledFromOneByteReads() throws Exception {
        Transport transport = Transport.NIO;
        PassageObservation.splitReads(1);
        try (TcpServer server = TcpServer.start(transport, type -> STATUS_OK);
                Peer client = Peer.connect(transport, server.port(), null)) {
            Peer serverSide = server.awaitPeer();

            RequestResult result = await(client.request(APPLICATION_TYPE, Fixtures.payload("hello")));
            Wire.Incoming received = serverSide.awaitRequest();

            assertThat(received.type()).isEqualTo(APPLICATION_TYPE);
            assertThat(received.payload()).isEqualTo(Fixtures.payload("hello"));
            assertThat(result.status()).isEqualTo(STATUS_OK);
            assertThat(result.payload()).isEqualTo(Fixtures.payload("ok"));
            serverSide.awaitReceivedBytes(client.passage().writtenBytes());
            client.awaitReceivedBytes(serverSide.passage().writtenBytes());
        }
    }
}
