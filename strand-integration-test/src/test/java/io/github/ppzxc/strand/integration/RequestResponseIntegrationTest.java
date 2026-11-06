package io.github.ppzxc.strand.integration;

import static io.github.ppzxc.strand.integration.Fixtures.APPLICATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.github.ppzxc.strand.core.internal.Event;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 규격 4.2절 스트림 식별을 실제 TCP 위에서 확인한다. */
class RequestResponseIntegrationTest {

    private static final int FIRST_TYPE = APPLICATION_TYPE;
    private static final int SECOND_TYPE = APPLICATION_TYPE + 1;
    /** 서버는 {@link #FIRST_TYPE} 요청에 이 status로, {@link #SECOND_TYPE} 요청에 다른 status로 응답한다. */
    private static final int STATUS_FOR_FIRST = 0;

    private static final int STATUS_FOR_SECOND = 1;

    /**
     * 응답이 요청을 낸 스트림에 매칭되지 않으면 클라이언트는 결과를 받지 못하거나 다른 요청의 결과로 처리한다. 요청 하나로는 이 오매칭을 구분할 수
     * 없어서, 응답을 기다리지 않고 요청 두 개를 겹쳐 낸다. 서버는 요청 {@code type}에 따라 서로 다른 {@code status}로 응답하므로, 각 결과가 자기
     * 스트림에 붙었는지 {@code status}로 알 수 있다. 클라이언트 번호 공간은 1, 3, 5...다(규격 4.2절).
     */
    @Test
    @DisplayName("겹쳐 낸 요청마다 응답이 자기 stream_id로 돌아온다 (규격 4.2)")
    void eachOverlappingRequestGetsItsOwnResponse() throws Exception {
        try (TestServer server =
                        TestServer.startResponding(type -> type == FIRST_TYPE ? STATUS_FOR_FIRST : STATUS_FOR_SECOND);
                TestClient client = TestClient.connect(server.port())) {
            client.request(FIRST_TYPE, Fixtures.payload("first"));
            client.request(SECOND_TYPE, Fixtures.payload("second"));

            Map<Integer, Integer> statusByStream = new HashMap<>();
            for (int i = 0; i < 2; i++) {
                Event result = client.awaitEvent(Event.Kind.RESULT);
                statusByStream.put(result.streamId(), result.status());
            }

            assertThat(statusByStream).containsOnly(entry(1, STATUS_FOR_FIRST), entry(3, STATUS_FOR_SECOND));
        }
    }
}
