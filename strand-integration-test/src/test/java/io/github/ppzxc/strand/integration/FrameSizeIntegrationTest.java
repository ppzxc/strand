package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static io.github.ppzxc.strand.integration.Fixtures.PING_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.nio.internal.ControlledPassage;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** 큰 애플리케이션 프레임이 제어 프레임을 막지 않는지 실제 TCP 위에서 확인한다(규격 3.6절). */
@ExtendWith(PassageObservation.class)
class FrameSizeIntegrationTest {

    /** 기본 {@code max_frame_size}(65,536)에 들어가는 큰 payload. */
    private static final int LARGE_PAYLOAD_BYTES = 60_000;

    /** 연결이 놀 때 PING을 내는 간격. 통로의 깨움을 붙잡은 동안 요청을 내고 이 시간이 흘러 PING이 나갈 때를 기다린다. */
    private static final Duration IDLE_PING_INTERVAL = Duration.ofMillis(1500);

    /**
     * 큰 요청과 PING이 같은 순간에 준비되면 아직 쓰기를 시작하지 않은 요청보다 PING이 먼저 소켓에 쓰인다. 요청이 먼저 나가면 PING은 큰 프레임 하나를 쓰는 동안 기다리게 된다.
     * 제품 API에는 PING을 내는 명령이 없어서 놀고 있는 연결의 유휴 PING으로 만든다. 통로의 깨움을 붙잡아 큰 요청이 전송 스레드에 닿지 않게 하고, 유휴 간격이 지나 전송 스레드가 스스로
     * 깨어나면 쌓인 요청과 PING이 한 바퀴에 함께 처리된다. 통로에 쓴 프레임 순서로 본다. 서버가 요청을 끝까지 받는 것도 함께 본다(쓰기를 시작한 프레임은 끝까지 쓴다).
     */
    @Test
    @DisplayName("[nio 전용] 큰 요청 뒤에 낸 PING이 아직 쓰지 않은 요청보다 먼저 나간다 (규격 3.6)")
    void pingIssuedBehindLargeRequestIsWrittenFirst() throws Exception {
        Transport transport = Transport.NIO;
        try (TcpServer server = TcpServer.start(transport);
                Peer client = Peer.connect(transport, server.port(), null, IDLE_PING_INTERVAL)) {
            Peer serverSide = server.awaitPeer();
            ControlledPassage passage = client.passage();
            passage.holdWakeups();
            int writtenBefore = passage.writtenFrameTypes().size();

            client.request(APPLICATION_TYPE, new byte[LARGE_PAYLOAD_BYTES]);
            // 깨움을 붙잡은 동안에는 요청이 처리되지 않는다. 유휴 간격이 지나 PING이 나가며 요청도 함께 처리된다.
            Peer.awaitUntil(
                    () -> passage.writtenFrameTypes().size() >= writtenBefore + 2,
                    "PING과 요청 프레임이 통로에 쓰이지 않았다: " + passage.writtenFrameTypes());
            passage.release();

            Wire.Incoming received = serverSide.awaitRequest();
            assertThat(received.type()).isEqualTo(APPLICATION_TYPE);
            assertThat(received.payload()).hasSize(LARGE_PAYLOAD_BYTES);
            List<Integer> types = passage.writtenFrameTypes();
            assertThat(types.subList(writtenBefore, writtenBefore + 2)).containsExactly(PING_TYPE, APPLICATION_TYPE);
        }
    }
}
