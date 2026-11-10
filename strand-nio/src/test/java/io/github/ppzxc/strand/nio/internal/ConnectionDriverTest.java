package io.github.ppzxc.strand.nio.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.core.internal.EndpointConfig;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** 전송 모듈의 interface(요청 → Future, 핸들러)로만 관찰한다. 두 연결은 in-memory 통로로 이어 붙인다. */
class ConnectionDriverTest {

    private static final int APPLICATION_TYPE = 256;
    private static final int STATUS_OK = 0;

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
    }

    /** 받은 요청 본문 앞에 "re:"를 붙여 응답하는 핸들러. */
    private static final RequestHandler ECHO =
            request -> request.respond(STATUS_OK, bytes("re:" + new String(request.body(), StandardCharsets.UTF_8)));

    @Test
    void clientRequestReceivesTheServerHandlersResponse() throws Exception {
        InMemoryPassage.Pair wire = InMemoryPassage.streams();
        try (ConnectionDriver server = ConnectionDriver.server(EndpointConfig.defaults(), wire.second())
                        .handler(Runnable::run, ECHO)
                        .start();
                ConnectionDriver client = ConnectionDriver.client(EndpointConfig.defaults(), wire.first())
                        .start()) {
            RequestResult result = await(client.request(APPLICATION_TYPE, bytes("hello")));

            assertThat(result.status()).isEqualTo(STATUS_OK);
            assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
            assertThat(result.body()).isEqualTo(bytes("re:hello"));
        }
    }
}
