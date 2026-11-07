package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Event;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 한 연결에서 양쪽이 서로 요청을 내는 경우를 실제 TCP 위에서 확인한다. */
class BidirectionalIntegrationTest {

    private static final int CLIENT_REQUEST_TYPE = APPLICATION_TYPE;
    private static final int SERVER_REQUEST_TYPE = APPLICATION_TYPE + 1;
    /** 서버가 클라이언트 요청에 응답할 status와, 클라이언트가 서버 요청에 응답할 status. 서로 달라야 어느 쪽 응답인지 가릴 수 있다. */
    private static final int STATUS_SERVER_RESPONDS = 0;

    private static final int STATUS_CLIENT_RESPONDS = 1;
    /** 규격 4.2절: 클라이언트 개시 스트림은 홀수, 서버 개시 스트림은 짝수다. 각자 처음 여는 번호는 1과 2다. */
    private static final int FIRST_CLIENT_STREAM_ID = 1;

    private static final int FIRST_SERVER_STREAM_ID = 2;

    /**
     * 요청과 응답은 번호 공간으로만 구분하므로(규격 4.2절), 양쪽이 동시에 요청을 내면 한 연결 안에서 홀수 번호와 짝수 번호가 섞이지 않아야 한다. 번호
     * 공간을 혼동하면 클라이언트가 서버의 요청을 자기 요청의 응답으로 처리하거나 그 반대가 되어, 도착한 요청의 {@code stream_id}나 결과의 {@code
     * status}가 어긋난다. 서버와 클라이언트가 서로 다른 {@code status}로 응답하므로 결과가 어느 방향의 응답인지 알 수 있다.
     */
    @Test
    @DisplayName("서버와 클라이언트가 동시에 요청을 내도 홀수·짝수 stream_id가 섞이지 않는다 (규격 4.2)")
    void bothSidesRequestWithoutMixingStreamNumbers() throws Exception {
        try (TestServer server = TestServer.startResponding(type -> STATUS_SERVER_RESPONDS);
                TestClient client = TestClient.connectResponding(server.port(), type -> STATUS_CLIENT_RESPONDS)) {
            Connection serverConnection = server.awaitConnection();
            client.request(CLIENT_REQUEST_TYPE, Fixtures.payload("from-client"));
            serverConnection.withEndpoint(e -> e.request(SERVER_REQUEST_TYPE, Fixtures.payload("from-server")));

            Event requestAtServer = serverConnection.awaitEvent(Event.Kind.REQUEST_RECEIVED);
            Event requestAtClient = client.awaitEvent(Event.Kind.REQUEST_RECEIVED);
            Event resultAtClient = client.awaitEvent(Event.Kind.RESULT);
            Event resultAtServer = serverConnection.awaitEvent(Event.Kind.RESULT);

            assertThat(requestAtServer.streamId()).isEqualTo(FIRST_CLIENT_STREAM_ID);
            assertThat(requestAtServer.type()).isEqualTo(CLIENT_REQUEST_TYPE);
            assertThat(requestAtClient.streamId()).isEqualTo(FIRST_SERVER_STREAM_ID);
            assertThat(requestAtClient.type()).isEqualTo(SERVER_REQUEST_TYPE);
            assertThat(resultAtClient.streamId()).isEqualTo(FIRST_CLIENT_STREAM_ID);
            assertThat(resultAtClient.status()).isEqualTo(STATUS_SERVER_RESPONDS);
            assertThat(resultAtServer.streamId()).isEqualTo(FIRST_SERVER_STREAM_ID);
            assertThat(resultAtServer.status()).isEqualTo(STATUS_CLIENT_RESPONDS);
        }
    }
}
