package io.github.ppzxc.strand.examples;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.ppzxc.strand.core.internal.Certainty;
import io.github.ppzxc.strand.nio.internal.RequestResult;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 서버 예제와 TCP 클라이언트 예제를 실제로 함께 돌려 요청 하나를 주고받는다. 포트는 운영체제가 정하는 임시 포트(0)이고, 기다림은 예제 안의 상한
 * 시간뿐이다.
 */
class TcpExamplesTest {

    private ServerExample.Server server;

    @BeforeEach
    void startServer() throws Exception {
        server = ServerExample.start(0);
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    private static void assertEchoed(RequestResult result) {
        assertThat(result.certainty()).isEqualTo(Certainty.RESPONSE);
        assertThat(result.status()).isEqualTo(ExampleSupport.STATUS_OK);
        assertThat(new String(result.body(), StandardCharsets.UTF_8)).isEqualTo("echo:hello");
    }

    @Test
    void theBlockingTcpClientAndTheServerExchangeOneRequest() throws Exception {
        assertEchoed(BlockingTcpExample.run("localhost", server.port()));
    }

    @Test
    void theNioTcpClientAndTheServerExchangeOneRequest() throws Exception {
        assertEchoed(NioTcpExample.run("localhost", server.port()));
    }
}
