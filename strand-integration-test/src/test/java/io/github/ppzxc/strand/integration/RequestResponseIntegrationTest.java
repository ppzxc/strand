package io.github.ppzxc.strand.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Event;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 규격 4.2절 스트림 식별을 실제 TCP 위에서 확인한다. */
class RequestResponseIntegrationTest {

    private static final int APPLICATION_TYPE = 256;
    private static final int STATUS_OK = 0;
    /** 규격 4.2절: 클라이언트가 처음 여는 스트림은 1번이다. */
    private static final int FIRST_CLIENT_STREAM_ID = 1;

    /**
     * 응답이 요청을 낸 스트림에 매칭되지 않으면 클라이언트는 결과를 받지 못하거나 다른 요청의 결과로 처리한다. 서버가 받은 요청의 {@code stream_id}와
     * 클라이언트가 받은 결과의 {@code stream_id}가 같고, 클라이언트 번호 공간(홀수)에서 시작하는지 본다.
     */
    @Test
    @DisplayName("서버 응답이 요청과 같은 stream_id로 클라이언트에 돌아온다 (규격 4.2)")
    void responseMatchesRequestStream() throws Exception {
        try (TestServer server = TestServer.startResponding(STATUS_OK);
                TestClient client = TestClient.connect(server.port())) {
            client.request(APPLICATION_TYPE, "hello".getBytes(StandardCharsets.UTF_8));

            Event received = server.awaitConnection().awaitEvent(Event.Kind.REQUEST_RECEIVED);
            Event result = client.awaitEvent(Event.Kind.RESULT);

            assertThat(received.streamId()).isEqualTo(FIRST_CLIENT_STREAM_ID);
            assertThat(result.streamId()).isEqualTo(received.streamId());
            assertThat(result.status()).isEqualTo(STATUS_OK);
        }
    }
}
