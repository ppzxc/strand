package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.PING_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.nio.internal.IncomingRequest;
import io.github.ppzxc.strand.nio.internal.TcpPassage;
import java.nio.channels.SocketChannel;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 큰 애플리케이션 프레임이 제어 프레임을 막지 않는지 실제 TCP 위에서 확인한다(규격 3.6절). */
class FrameSizeIntegrationTest {

    /** 기본 {@code max_frame_size}(65,536)에 들어가는 큰 payload. */
    private static final int LARGE_PAYLOAD_BYTES = 60_000;

    /**
     * 큰 요청과 PING을 같은 순간에 내면 아직 쓰기를 시작하지 않은 요청보다 PING이 먼저 소켓에 쓰인다. 요청을 먼저 냈어도 마찬가지다. 요청이 먼저
     * 나가면 PING은 큰 프레임 하나를 쓰는 동안 기다리게 된다. 두 명령이 전송 스레드의 같은 한 바퀴에 처리되도록 통로의 깨움을 붙잡았다가 풀고, 통로에
     * 쓴 프레임 순서로 본다. 서버가 요청을 끝까지 받는 것도 함께 본다(쓰기를 시작한 프레임은 끝까지 쓴다). 보낸 프레임 수는 전송 모듈의 관찰 지점으로도 센다.
     */
    @Test
    @DisplayName("큰 요청 뒤에 낸 PING이 아직 쓰지 않은 요청보다 먼저 나간다 (규격 3.6)")
    void pingIssuedBehindLargeRequestIsWrittenFirst() throws Exception {
        try (TcpServer server = TcpServer.start()) {
            SocketChannel channel = Peer.open(server.port());
            ControlledPassage passage = new ControlledPassage(TcpPassage.of(channel));
            try (Peer client = Peer.client(passage, null)) {
                Peer serverSide = server.awaitPeer();
                client.awaitAuthenticated();
                passage.holdWakeups();
                int writtenBefore = passage.writtenFrameTypes().size();
                long framesBefore = client.driver().writtenFrames();

                client.request(APPLICATION_TYPE, new byte[LARGE_PAYLOAD_BYTES]);
                client.driver().ping(new byte[8]);
                passage.release();

                IncomingRequest received = serverSide.awaitRequest();
                assertThat(received.type()).isEqualTo(APPLICATION_TYPE);
                assertThat(received.body()).hasSize(LARGE_PAYLOAD_BYTES);
                List<Integer> types = passage.writtenFrameTypes();
                assertThat(types.subList(writtenBefore, types.size())).containsExactly(PING_TYPE, APPLICATION_TYPE);
                // 카운터는 통로에 다 쓴 뒤에 늘므로 서버가 받은 것과 같은 순간을 보장하지 않는다. 상한 안에서 기다린다.
                Peer.awaitUntil(
                        () -> client.driver().writtenFrames() - framesBefore == 2,
                        "보낸 프레임 수가 2가 되지 않았다: " + (client.driver().writtenFrames() - framesBefore));
            }
        }
    }
}
