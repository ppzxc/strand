package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Event;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 큰 애플리케이션 프레임이 제어 프레임을 막지 않는지 실제 TCP 위에서 확인한다(규격 3.6절). */
class FrameSizeIntegrationTest {

    /** 규격 2.3절: PING의 type. */
    private static final int PING_TYPE = 2;

    /** 기본 {@code max_frame_size}(65,536)에 들어가는 큰 payload. */
    private static final int LARGE_PAYLOAD_BYTES = 60_000;

    /**
     * 큰 요청과 PING을 같은 순간에 내면 아직 쓰기를 시작하지 않은 요청보다 PING이 먼저 소켓에 쓰인다. 요청을 먼저 냈어도 마찬가지다. 요청이 먼저
     * 나가면 PING은 큰 프레임 하나를 쓰는 동안 기다리게 된다. 소켓에 쓴 프레임 순서로 보고, 서버가 요청을 끝까지 받는 것도 함께 본다(쓰기를 시작한
     * 프레임은 끝까지 쓴다).
     */
    @Test
    @DisplayName("큰 요청 뒤에 낸 PING이 아직 쓰지 않은 요청보다 먼저 나간다 (규격 3.6)")
    void pingIssuedBehindLargeRequestIsWrittenFirst() throws Exception {
        try (TestServer server = TestServer.start();
                TestClient client = TestClient.connect(server.port())) {
            Connection serverConnection = server.awaitConnection();
            client.connection().awaitAuthenticated();
            int writtenBefore = client.connection().writtenFrameTypes().size();

            client.connection().withEndpoint(endpoint -> {
                endpoint.request(APPLICATION_TYPE, new byte[LARGE_PAYLOAD_BYTES]);
                endpoint.ping(new byte[8]);
            });

            Event received = serverConnection.awaitEvent(Event.Kind.REQUEST_RECEIVED);
            assertThat(received.type()).isEqualTo(APPLICATION_TYPE);
            List<Integer> types = client.connection().writtenFrameTypes();
            assertThat(types.subList(writtenBefore, types.size())).containsExactly(PING_TYPE, APPLICATION_TYPE);
        }
    }
}
