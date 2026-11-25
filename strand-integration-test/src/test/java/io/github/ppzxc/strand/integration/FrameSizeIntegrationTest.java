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

@ExtendWith(PassageObservation.class)
class FrameSizeIntegrationTest {

    private static final int LARGE_PAYLOAD_BYTES = 60_000;

    private static final Duration IDLE_PING_INTERVAL = Duration.ofMillis(1500);

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
